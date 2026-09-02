#!/usr/bin/env python
"""
rdb_to_mat_bcm
==============

Reads every Redis stream this graph (mediaPlayerTask.yaml, BCM variant)
produces and writes one <stream>.mat per stream -- no per-trial slicing or
cross-stream alignment, since this graph only ever runs one continuous
media-playback "trial" (see mediaPlayerTask), not many discrete trials like
the rdb_to_mat_omni_brown/stanford reference graphs this was adapted from.

Writes:
  - metadata.txt        -- human-readable session metadata (best-effort; see
                            note in get_metadata() about the 'metadata' stream)
  - session_info.mat     -- the playback start/end timestamps pulled out of
                            trial_info, for slicing other streams in MATLAB
  - <stream>.mat          -- one per stream in SCHEMA (task_state, trial_info,
                            reref_neural, sbp_1, sbp_2, thresh_cross_1/2/3,
                            binned_spikes). Every data field is written
                            channels-first: sbp_*/thresh_cross_*/binned_spikes
                            are already one value per channel per entry, so
                            their 'samples'/'crossings' field is transposed
                            from (N_entries, n_channels) to (n_channels,
                            N_entries). reref_neural's 'samples' is a
                            per-entry (n_channels, samp_per_stream) buffer
                            flattened channel-major (see cerebusAdapterBCM.py)
                            -- unflattened and concatenated across entries
                            into one continuous (neural_n_channels,
                            N_entries*samp_per_stream) trace, using the
                            neural_n_channels/samp_per_stream params below;
                            its 'timestamps' field is flattened to match, one
                            timestamp per trace column.
  - nsp_neural.mat        -- opt-in (include_continuous_neural param), the
                            raw 30kHz acquisition stream -- large, off by
                            default. Same continuous-trace reshape as
                            reref_neural, to (neural_n_channels,
                            N_entries*samp_per_stream).
  - <analog_stream>.mat   -- opt-in (include_analog param), cerebusAdapterBCM's
                            analog-channel stream (e.g. nsp_analog), reshaped
                            to (analog_n_channels, N_entries*samp_per_stream).
  - <prefix>_channel_map.mat -- opt-in (include_channel_map param), the
                            per-Sample-Group channel identity table written
                            by cerebusAdapterBCM
  - missing_streams.txt   -- only when expected streams were absent

Self-contained (no brown_brand dependency) -- CLI args and supergraph
parameter lookup follow the same -n/-i/-p/-s convention as every other BRAND
derivative in this repo (see brand-rice/derivatives/saveRDB/saveRDB.py).
"""

import argparse
import json
import logging
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np
import scipy.io
from redis import ConnectionError, Redis

# RedisLoggingHandler was missing from BRAND/BRAND/lib/python/brand/redis.py
# (never committed on any branch, despite being imported by ~20 derivative
# scripts across other labs' brand-modules submodules) until it was added
# there on 2026-08-19 during bcm-emu-1 setup -- see the docstring on that
# class for the full story. If this import ever breaks again, check that
# file first before assuming this script/module did something wrong.
from brand.redis import RedisLoggingHandler


###############################################################################
# CLI + supergraph parameter lookup
###############################################################################

def _parse_cli():
    ap = argparse.ArgumentParser()
    ap.add_argument('-n', '--nickname', type=str, required=True)
    ap.add_argument('-i', '--redis_host', type=str, required=True)
    ap.add_argument('-p', '--redis_port', type=int, required=True)
    ap.add_argument('-s', '--redis_socket', type=str, required=False)
    return ap.parse_args()


def _params_from_supergraph(r, nickname):
    try:
        _, entry = r.xrevrange(b'supergraph_stream', '+', '-', 1)[0]
    except IndexError:
        logging.error('No model published to supergraph_stream. Exiting.')
        sys.exit(1)
    supergraph = json.loads(entry[b'data'].decode())
    try:
        return supergraph['derivatives'][nickname]['parameters']
    except KeyError:
        logging.warning(f"No '{nickname}' derivative configuration in current graph.")
        return {}


###############################################################################
# Redis stream decoding schema
###############################################################################
# Each schema entry maps a (binary) field name to a decoder spec:
#   ('buf', dtype)     -> np.frombuffer(bytes, dtype=dtype) as an array
#   ('scalar', dtype)  -> np.frombuffer(bytes, dtype=dtype)[0] as a number
#   ('str',)           -> bytes.decode() as a Python str
#   ('str_int',)       -> bytes.decode() then int() -- for fields redis-py
#                         encoded from a plain Python int/float rather than
#                         .tobytes() (e.g. cerebusAdapterBCM's channel_map
#                         entries -- see its _write_channel_map)
#   ('str_float',)      -> same, but float()
#   ('json',)           -> json.loads(bytes) then json.dumps back to a string
#                         (keeps the whole sync dict as one string per row)
#
# Every field below is read with a fill-in-if-missing default (see
# read_stream) rather than being skipped when absent from a given entry --
# task_state entries carry different optional fields depending on 'state'
# (e.g. only 'start_time' entries have media_file_path), and skipping would
# silently misalign that field's column against the other columns' row
# count. Padding keeps every column exactly one value per Redis entry.

SCHEMA = {
    'task_state': {
        b'state':                 ('str',),
        b'redis_timestamp':       ('scalar', np.uint64),
        b'nsp_neural_timestamp':  ('scalar', np.uint64),
        b'nsp_analog_timestamp':  ('scalar', np.uint64),
        b'sync':                  ('json',),
        b'ts':                    ('scalar', np.uint64),
        b'media_file_path':       ('str',),      # only present on 'start_time' rows
        b'time_pos':              ('scalar', np.float64),  # only on 'playing' rows
        b'end_reason':            ('str',),      # only on 'return_time' rows
    },
    'trial_info': {
        b'media_file_path':             ('str',),
        b'trial_start_redis_time':      ('scalar', np.uint64),
        b'trial_start_nsp_neural_time': ('scalar', np.uint64),
        b'trial_start_nsp_analog_time': ('scalar', np.uint64),
        b'trial_end_redis_time':        ('scalar', np.uint64),
        b'trial_end_nsp_neural_time':   ('scalar', np.uint64),
        b'trial_end_nsp_analog_time':   ('scalar', np.uint64),
        b'end_reason':                  ('str',),
        b'finished_naturally':          ('scalar', np.uint8),
        b'sync':                        ('json',),
        b'ts':                          ('scalar', np.uint64),
    },
    'reref_neural': {
        b'samples':     ('buf', np.float32),   # (chan_per_stream, samp_per_stream) flattened
        b'timestamps':  ('buf', np.uint64),    # per-sample timestamps, passed through from nsp_neural
        b'tracking_id': ('scalar', np.uint64),
    },
    'sbp_1': {
        b'samples':    ('buf', np.float32),
        b'timestamps': ('scalar', np.uint64),
        b'sync':       ('json',),
        b'ts':         ('scalar', np.uint64),
    },
    'sbp_2': {
        b'samples':    ('buf', np.float32),
        b'timestamps': ('scalar', np.uint64),
        b'sync':       ('json',),
        b'ts':         ('scalar', np.uint64),
    },
    'thresh_cross_1': {
        b'crossings':  ('buf', np.int16),
        b'timestamps': ('scalar', np.uint64),
        b'sync':       ('json',),
        b'ts':         ('scalar', np.uint64),
    },
    'thresh_cross_2': {
        b'crossings':  ('buf', np.int16),
        b'timestamps': ('scalar', np.uint64),
        b'sync':       ('json',),
        b'ts':         ('scalar', np.uint64),
    },
    'thresh_cross_3': {
        b'crossings':  ('buf', np.int16),
        b'timestamps': ('scalar', np.uint64),
        b'sync':       ('json',),
        b'ts':         ('scalar', np.uint64),
    },
    'binned_spikes': {
        b'samples': ('buf', np.float32),
        b'i':       ('scalar', np.uint64),
        b'sync':    ('json',),
        b'ts':      ('scalar', np.uint64),
    },
}

# cerebusAdapterBCM's raw per-Sample-Group acquisition schema -- same shape
# for any group it streams (timestamps/samples/tracking_id), so this is
# reused for both the raw neural dump (include_continuous_neural) and the
# analog-input-bank dump (include_analog) below -- only the stream name and
# opt-in flag differ.
CONTINUOUS_NEURAL_SCHEMA = {
    b'timestamps':  ('buf', np.uint64),
    b'samples':     ('buf', np.int16),
    b'tracking_id': ('scalar', np.uint64),
}
ANALOG_SCHEMA = CONTINUOUS_NEURAL_SCHEMA

# cerebusAdapterBCM's per-Sample-Group channel identity table -- opt-in via
# include_channel_map. One row per active Sample Group, not per time step.
CHANNEL_MAP_SCHEMA = {
    b'group':        ('str_int',),
    b'rate_hz':      ('str_float',),
    b'stream_name':  ('str',),
    b'n_channels':   ('str_int',),
    b'channel_ids':  ('buf', np.int32),
    b'labels':       ('json',),
}


###############################################################################
# Fill-in-if-missing defaults, keyed by decoder kind
###############################################################################

_FILL_VALUE = {
    'buf':      lambda spec: np.array([], dtype=spec[1]),
    'scalar':   lambda spec: spec[1](0),
    'str':      lambda spec: '',
    'str_int':  lambda spec: 0,
    'str_float': lambda spec: 0.0,
    'json':     lambda spec: '',
}


def _decode_field(raw, spec):
    kind = spec[0]
    if kind == 'buf':
        return np.frombuffer(raw, dtype=spec[1])
    if kind == 'scalar':
        arr = np.frombuffer(raw, dtype=spec[1])
        return arr[0] if arr.size else spec[1](0)
    if kind == 'str':
        try:
            return raw.decode()
        except (AttributeError, UnicodeDecodeError):
            return str(raw)
    if kind == 'str_int':
        try:
            return int(raw.decode())
        except (AttributeError, ValueError):
            return 0
    if kind == 'str_float':
        try:
            return float(raw.decode())
        except (AttributeError, ValueError):
            return 0.0
    if kind == 'json':
        # Re-serialize as a JSON string so savemat stores one string per row.
        try:
            return json.dumps(json.loads(raw))
        except (ValueError, TypeError):
            try:
                return raw.decode()
            except Exception:
                return ''
    raise ValueError(f'unknown schema spec kind: {kind}')


def clean_redis_timestamp(entry_id):
    """Extract the integer ms timestamp from a Redis stream ID like b'1712345-0'."""
    return int(entry_id.decode().split('-')[0])


def read_stream(r, stream_name, schema, page=10000):
    """Read a whole stream into dict[field_name -> list], one value per row.

    Every field in `schema` gets exactly one entry per Redis row -- missing
    fields are filled with a schema-appropriate default rather than skipped,
    so every column stays aligned to the same row count (see SCHEMA comment
    for why this matters for task_state). Pages through XRANGE in
    `page`-sized chunks so large streams never sit in one Redis response.
    Returns None if the stream is empty or doesn't exist.
    """
    frames = defaultdict(list)
    last_id = b'-'
    saw_any = False
    while True:
        batch = r.xrange(stream_name, min=last_id, max=b'+', count=page)
        if not batch:
            break
        full_len = len(batch)
        # xrange with min=<ID> is inclusive -- skip the already-seen boundary row
        if last_id != b'-' and batch[0][0] == last_id:
            batch = batch[1:]
        if not batch:
            break
        saw_any = True
        for entry_id, entry in batch:
            frames['redis_timestamps'].append(clean_redis_timestamp(entry_id))
            for key, spec in schema.items():
                field = key.decode()
                if key in entry:
                    try:
                        frames[field].append(_decode_field(entry[key], spec))
                        continue
                    except Exception as e:
                        logging.warning(
                            f"failed decoding '{stream_name}.{field}' at {entry_id}: {e}")
                frames[field].append(_FILL_VALUE[spec[0]](spec))
            last_id = entry_id
        if full_len < page:
            break
    if not saw_any:
        logging.warning(f"stream '{stream_name}' is empty or missing -- skipping")
        return None
    return dict(frames)


def format_dictionary_matrices(frames, assume_uniform_buf=True):
    """Stack per-field lists into numpy arrays of shape (N, ...).

    assume_uniform_buf: True for per-timestep streams, where every row of a
    'buf' field is supposed to be the same length (e.g. n_channels for
    sbp_1, n_channels*samp_per_stream for reref_neural) -- a length mismatch
    there means a decode failure, not real data (see the warning below).
    False for streams where rows are genuinely, intentionally different
    lengths (channel_map: each row is one Sample Group's channel_ids, and
    different groups have different channel counts) -- for those, ragged
    rows are stored as a dtype=object array instead, same as before.
    """
    out = {}
    for key, values in frames.items():
        if not values:
            continue
        first = values[0]
        if isinstance(first, np.ndarray):
            if first.size == 0:
                values = [np.array([np.nan]) if v.size == 0 else v for v in values]
            lengths = {v.size for v in values}
            if len(lengths) > 1 and not assume_uniform_buf:
                # Genuinely ragged, expected data (see docstring) -- store
                # as an object array rather than failing.
                arr = np.empty(len(values), dtype=object)
                for i, v in enumerate(values):
                    arr[i] = v
                out[key] = arr
                continue
            if len(lengths) > 1:
                # Every 'buf'-type field in this file's SCHEMA is a
                # fixed-width per-channel/per-sample buffer -- a length that
                # doesn't match the rest almost always means read_stream's
                # decode fallback fired for that one entry (its 0-length
                # _FILL_VALUE default, see there), not real per-entry
                # variability. Leaving this ragged would force
                # np.vstack -> the dtype=object fallback below, and
                # scipy.io.savemat has been observed to handle that path
                # badly on this box -- a clean TypeError one run, a hard
                # segfault (confirmed via dmesg) another run on otherwise
                # identical malformed input. So normalize instead of ever
                # building a ragged/object array: replace off-length rows
                # with a fill value at the modal (most common) length.
                modal_len = Counter(v.size for v in values).most_common(1)[0][0]
                n_bad = sum(1 for v in values if v.size != modal_len)
                fill_val = np.nan if np.issubdtype(first.dtype, np.floating) else 0
                logging.warning(
                    f"field decoded to {len(lengths)} different lengths "
                    f"across entries ({sorted(lengths)}); {n_bad} entries "
                    f"don't match the modal length {modal_len} -- replacing "
                    f"those {n_bad} rows with a constant {fill_val!r} fill "
                    f"so the .mat write can't fail/crash on ragged data. "
                    f"Treat those rows as missing, not real zeros/NaNs.")
                fill_row = np.full(modal_len, fill_val, dtype=first.dtype)
                values = [v if v.size == modal_len else fill_row for v in values]
            arr = np.vstack(values)
            if arr.ndim == 1:
                arr = arr[:, np.newaxis]
        elif isinstance(first, (str, bytes)):
            arr = np.array(values, dtype=object)
        else:
            arr = np.array(values)[:, np.newaxis]
        out[key] = arr
    return out


def save_stream_mat(frames, stream_name, savepath, reshape=None, transpose_field=None,
                     assume_uniform_buf=True):
    """Two mutually-exclusive ways to turn a field into a (n_channels,
    n_samples) matrix instead of the raw (N_entries, ...) layout
    format_dictionary_matrices produces -- pass at most one:

    reshape=(field_name, n_channels, samp_per_stream): for a field that's a
    per-entry (n_channels, samp_per_stream) buffer flattened channel-major
    (cerebusAdapterBCM.py/re_reference.py's convention: each row is
    [ch0_s0..ch0_s(samp-1), ch1_s0.., ...]) -- e.g. reref_neural/nsp_neural/
    nsp_analog's 'samples'. Un-flattens each row to (n_channels,
    samp_per_stream), then concatenates all N_entries along the sample axis
    (entries are already in chronological order from read_stream/XRANGE) into
    one continuous (n_channels, N_entries*samp_per_stream) trace -- a 2-D
    matrix, not a 3-D per-entry stack. If a 'timestamps' field is also
    present with the matching (N_entries, samp_per_stream) shape, it's
    flattened the same way to a (N_entries*samp_per_stream,) vector so it
    stays aligned with the trace's columns.

    transpose_field=field_name: for a field that's already exactly one
    value per channel per entry -- no samples axis to recover (e.g.
    sbp_1/sbp_2's 'samples', thresh_cross_*'s 'crossings', binned_spikes'
    'samples') -- just transposes (N_entries, n_channels) to (n_channels,
    N_entries) so every stream ends up channels-first, consistent with the
    reshape case above.

    assume_uniform_buf: passed through to format_dictionary_matrices -- set
    False for channel_map, whose rows are legitimately different lengths
    per Sample Group.
    """
    formatted = format_dictionary_matrices(frames, assume_uniform_buf=assume_uniform_buf)
    if not formatted:
        logging.warning(f"stream '{stream_name}' decoded to no rows -- skipping write")
        return
    if reshape is not None:
        field, n_channels, samp_per_stream = reshape
        if field in formatted:
            arr = formatted[field]
            expected_len = n_channels * samp_per_stream
            if arr.ndim == 2 and arr.shape[1] == expected_len:
                n_entries = arr.shape[0]
                per_entry = arr.reshape(n_entries, n_channels, samp_per_stream)
                formatted[field] = np.ascontiguousarray(
                    per_entry.transpose(1, 0, 2)).reshape(n_channels, n_entries * samp_per_stream)
                ts = formatted.get('timestamps')
                if ts is not None and ts.shape == (n_entries, samp_per_stream):
                    formatted['timestamps'] = ts.reshape(-1)
            else:
                logging.warning(
                    f"'{stream_name}.{field}' has shape {arr.shape}, expected "
                    f"(N, {expected_len}) for a ({n_channels}, {samp_per_stream}) "
                    f"reshape -- leaving it flat. Check neural_n_channels/"
                    f"analog_n_channels/samp_per_stream derivative params "
                    f"against what the graph is actually configured to send.")
    if transpose_field is not None and transpose_field in formatted:
        arr = formatted[transpose_field]
        if arr.ndim == 2:
            formatted[transpose_field] = arr.T
    scipy.io.savemat(savepath / f'{stream_name}.mat', formatted, do_compression=True)
    # redis_timestamps is always present and never reshaped/transposed above,
    # so its length is the true entry count regardless of what shape the
    # data fields ended up in -- unlike grabbing len() of an arbitrary field,
    # which after a reshape/transpose is n_channels or total_samples, not N.
    n_entries = len(formatted.get('redis_timestamps', next(iter(formatted.values()))))
    logging.info(f"wrote {stream_name}.mat ({n_entries} entries)")


###############################################################################
# Metadata / session info
###############################################################################

def get_metadata(r):
    # NOTE: nothing in this graph's nodes (or BRAND/lib/python/brand/
    # supervisor.py) writes to a 'metadata' Redis stream -- this almost
    # always falls through to the defaults below. Kept in case some other
    # piece of the deployment does populate it; harmless if not.
    data = r.xrevrange(b'metadata', count=1)
    if not data:
        logging.warning("'metadata' stream not present -- using defaults")
        return {
            'participant': 'unknown',
            'session_name': 'unknown',
            'graph_name': '',
            'block_num': -1,
            'start_time': 0,
        }
    _, entry = data[0]

    def _get_str(key, default=''):
        v = entry.get(key)
        return v.decode() if v is not None else default

    def _get_uint64(key, default=0):
        v = entry.get(key)
        if v is None:
            return default
        try:
            return int(np.frombuffer(v, dtype=np.uint64)[0])
        except Exception:
            try:
                return int(v.decode())
            except Exception:
                return default

    return {
        'participant':  _get_str(b'participant', 'unknown'),
        'session_name': _get_str(b'session_name', 'unknown'),
        'graph_name':   _get_str(b'graph_name'),
        'block_num':    int(entry.get(b'block_num', b'-1').decode()) if b'block_num' in entry else -1,
        'start_time':   _get_uint64(b'startTime'),
    }


def save_metadata_txt(savepath, metadata):
    with open(savepath / 'metadata.txt', 'wt') as f:
        for k, v in metadata.items():
            f.write(f'{k}: {v}\n')


def save_session_info_mat(trial_info_raw, savepath):
    """The playback start/end timestamps out of trial_info, for slicing
    other streams' .mat files in MATLAB by hand -- this graph only ever has
    one continuous 'trial' (one media_file_path playback), so there's no
    per-trial struct array to build here."""
    if trial_info_raw is None:
        logging.warning('trial_info stream missing -- no session_info.mat will be written')
        return
    formatted = format_dictionary_matrices(trial_info_raw)
    if not formatted:
        logging.warning('trial_info decoded to no rows -- no session_info.mat will be written')
        return
    scipy.io.savemat(savepath / 'session_info.mat', formatted, do_compression=True)
    logging.info('wrote session_info.mat')


def get_savepath(r, metadata, derivative_name):
    """Build <rdb_parent>/<derivative_name>/YYYYMMDD_HHMMSS_block_N/ and mkdir."""
    try:
        rdb_dir = Path(r.config_get('dir').get('dir', '.'))
    except Exception:
        rdb_dir = Path('.')

    base = rdb_dir.parent / derivative_name
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    out = base / f"{stamp}_block_{metadata['block_num']}"
    out.mkdir(parents=True, exist_ok=True)
    return out


###############################################################################
# Orchestrator
###############################################################################

def _connect(args):
    try:
        r = Redis(host=args.redis_host, port=args.redis_port,
                  unix_socket_path=args.redis_socket, retry_on_timeout=True)
        r.ping()
    except ConnectionError as e:
        logging.error(f'Redis connection failed: {e}')
        sys.exit(1)
    return r


def run():
    args = _parse_cli()
    name = args.nickname

    logging.basicConfig(
        format=f'[{name}] %(levelname)s: %(message)s',
        level=logging.INFO,
        stream=sys.stdout,
    )

    r = _connect(args)
    logging.getLogger().addHandler(RedisLoggingHandler(r, name))
    logging.info('connected to Redis')

    params = _params_from_supergraph(r, name)
    include_continuous_neural = bool(params.get('include_continuous_neural', False))
    include_channel_map = bool(params.get('include_channel_map', False))
    include_analog = bool(params.get('include_analog', False))
    continuous_neural_stream = str(params.get('continuous_neural_stream', 'nsp_neural'))
    channel_map_stream = str(params.get('channel_map_stream', 'nsp1_in_channel_map'))
    analog_stream = str(params.get('analog_stream', 'nsp_analog'))
    # For undoing the channel-major (n_channels, samp_per_stream) flatten on
    # reref_neural/nsp_neural/nsp_analog's 'samples' field -- see
    # save_stream_mat's reshape param. Keep these in sync with nsp1_in's
    # n_neural_channels/samples_per_entry and re_reference's samp_per_stream
    # in the graph YAML; they're independent params here (not read from
    # those nodes' own config) so this derivative doesn't have to know
    # anything about other nodes' parameter names.
    neural_n_channels = int(params.get('neural_n_channels', 256))
    analog_n_channels = int(params.get('analog_n_channels', 16))
    samp_per_stream = int(params.get('samp_per_stream', 30))
    logging.info(
        f'params: include_continuous_neural={include_continuous_neural}, '
        f'include_channel_map={include_channel_map}, '
        f'include_analog={include_analog}, '
        f'neural_n_channels={neural_n_channels}, analog_n_channels={analog_n_channels}, '
        f'samp_per_stream={samp_per_stream}')

    metadata = get_metadata(r)
    savepath = get_savepath(r, metadata, name)
    logging.info(f'writing outputs to {savepath}')

    save_metadata_txt(savepath, metadata)

    # Read every stream once
    raw = {}
    for stream_name, schema in SCHEMA.items():
        raw[stream_name] = read_stream(r, stream_name, schema)

    # Write per-stream .mat files, channels-first throughout: reref_neural's
    # 'samples' is a per-entry (n_channels, samp_per_stream) channel-major
    # buffer -> reshape to a continuous (n_channels, N_entries*samp_per_stream)
    # trace. sbp_*/thresh_cross_*/binned_spikes are already one value per
    # channel per entry (no samples axis) -> just transpose to
    # (n_channels, N_entries).
    reshape_specs = {'reref_neural': ('samples', neural_n_channels, samp_per_stream)}
    transpose_specs = {
        'sbp_1': 'samples', 'sbp_2': 'samples',
        'thresh_cross_1': 'crossings', 'thresh_cross_2': 'crossings', 'thresh_cross_3': 'crossings',
        'binned_spikes': 'samples',
    }
    for stream_name, frames in raw.items():
        if frames is None:
            continue
        save_stream_mat(frames, stream_name, savepath,
                         reshape=reshape_specs.get(stream_name),
                         transpose_field=transpose_specs.get(stream_name))

    missing = [s for s in SCHEMA if raw.get(s) is None]
    if missing:
        logging.warning(f'streams missing/empty: {missing}')
        try:
            with open(savepath / 'missing_streams.txt', 'wt') as f:
                for s in missing:
                    f.write(f'{s}\n')
        except Exception as e:
            logging.warning(f'could not write missing_streams.txt: {e}')

    save_session_info_mat(raw.get('trial_info'), savepath)

    # Optional raw-neural dump (read_stream pages to avoid a multi-GB allocation)
    if include_continuous_neural:
        cn = read_stream(r, continuous_neural_stream, CONTINUOUS_NEURAL_SCHEMA)
        if cn is not None:
            save_stream_mat(cn, continuous_neural_stream, savepath,
                             reshape=('samples', neural_n_channels, samp_per_stream))

    if include_channel_map:
        cm = read_stream(r, channel_map_stream, CHANNEL_MAP_SCHEMA)
        if cm is not None:
            save_stream_mat(cm, channel_map_stream, savepath, assume_uniform_buf=False)

    # Optional analog-input-bank dump (e.g. PS4 audio) -- kept separate from
    # the neural pipeline (SCHEMA above), since it never goes through
    # re_reference/threshold/bp extraction, just straight from nsp1_in to Redis.
    if include_analog:
        an = read_stream(r, analog_stream, ANALOG_SCHEMA)
        if an is not None:
            save_stream_mat(an, analog_stream, savepath,
                             reshape=('samples', analog_n_channels, samp_per_stream))

    try:
        r.quit()
    except Exception:
        pass

    logging.info('finished saving data.')


if __name__ == '__main__':
    run()
