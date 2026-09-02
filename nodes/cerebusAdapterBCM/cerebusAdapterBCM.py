#!/usr/bin/env python
# cerebusAdapterBCM.py
#
# NSP data-acquisition node built on pycbsdk (CereLink SDK bindings) instead
# of hand-rolled raw-UDP packet parsing. Unlike cerebusAdapter/
# cerebusAdapterGem (brand-nsp), this node does not require packet_type/
# chan_per_stream/samp_freq/broadcast_ip/broadcast_device to be hand-specified
# in the graph YAML -- it connects to the NSP via cbsdk at startup and
# auto-discovers which Sample Groups are active and how many/which channels
# are in each, using the exact same live-query approach as
# poll_nsp_config.py. This also resolves the channel-ordering ambiguity that
# raw UDP parsing can't: cbsdk's get_group_channels() returns each Sample
# Group's channel list in the same order its packet payloads are delivered
# in, so which buffer column is which physical channel is known and recorded
# -- not guessed.
#
# Requires the `nsp_env` conda env (pycbsdk + redis + brand + cython all
# installed there -- see CLAUDE.md for why this isn't `rt`: pycbsdk requires
# Python >=3.9, rt is pinned to 3.8 for its Cython/gcc node-build toolchain).
#
# Neural vs. analog routing: a Sample Group is just a sampling-rate bucket --
# Central assigns a channel to a group purely based on that channel's
# configured continuous rate, so which channels land in which group can
# change any time someone edits Central's rate config (confirmed on this
# rig: setting the front-end channels to 30kHz merged them into the same
# group as the already-30kHz analog-input bank). Keying stream routing off
# group_id is therefore fragile. Instead, every active channel across every
# discovered group is classified by its real Blackrock channel_id -- a
# hardware-fixed fact (front-end/neural channels are always the
# lowest-numbered block, the analog-input bank always immediately after,
# per cbproto.h's global bank layout) -- against the n_neural_channels
# boundary (a graph parameter, portable to sites with a different neural
# channel count), and routed to neural_stream_name/analog_stream_name
# accordingly, regardless of how many Sample Groups they're spread across.

import gc
import json
import logging
import signal
import sys
import threading
import time

import numpy as np
from brand import BRANDNode
from pycbsdk import DeviceType, SampleRate, Session
from pycbsdk.session import RUNLEVEL_RUNNING


class CerebusAdapterBCM(BRANDNode):

    def __init__(self):
        super().__init__()
        self.session = None
        self.groups = {}  # group_id -> _GroupInfo
        self.parameter_initialization()
        self.connect_and_discover()
        signal.signal(signal.SIGINT, self.terminate)

    def parameter_initialization(self):
        self.device_address = self.parameters['device_address']
        self.client_interface = self.parameters.get('client_interface')
        self.shmem_instance = int(self.parameters.get('shmem_instance', 0))
        self.redis_output_prefix = self.parameters.get('redis_output_prefix', self.NAME)
        self.connect_timeout_s = float(self.parameters.get('connect_timeout_s', 10.0))
        self.channel_map_stream = f'{self.redis_output_prefix}_channel_map'
        # on_group_batch calls back with however many samples its internal
        # queue drain accumulated (~30 at 30kHz per pycbsdk's own
        # docstring, but not guaranteed exact) -- downstream nodes
        # (re_reference.py, bpExtraction.py) reshape `samples` assuming a
        # FIXED sample count per Redis entry (their own samp_per_stream
        # graph param), so batches of the wrong width must be buffered and
        # re-chunked here rather than written straight through.
        self.samples_per_entry = int(self.parameters.get('samples_per_entry', 30))
        # Real Blackrock channel_id boundary between neural front-end and
        # analog-input-bank channels: channel_id 1..n_neural_channels ->
        # neural_stream_name, everything else (channel_id >
        # n_neural_channels, or an invalid/padding id -- see _calibrate) ->
        # analog_stream_name. Default (256) matches this rig's front-end
        # channel count; a site with a different neural channel count only
        # needs to change this one number, not the graph's stream wiring.
        self.n_neural_channels = int(self.parameters.get('n_neural_channels', 256))
        self.neural_stream_name = self.parameters.get('neural_stream_name', 'nsp_neural')
        self.analog_stream_name = self.parameters.get('analog_stream_name', 'nsp_analog')

        if not self.client_interface:
            # Confirmed empirically on this rig: a session opened with only
            # device_address (no client_interface) can intermittently
            # receive a DIFFERENT NSP's data for the same group id (e.g.
            # NSP1 connected bare received group-5 batches with NSP2's
            # channel count, alternating with its own) -- there is no
            # "default NSP, no special config needed" case in practice here.
            # Always set client_interface explicitly per NSP.
            logging.warning(
                'No client_interface set -- this has been observed to let '
                'a different NSP\'s data leak into this session\'s streams '
                'on this rig. Set client_interface explicitly unless you\'ve '
                'verified this NSP/environment doesn\'t need it.')

    def connect_and_discover(self):
        # shmem_instance must be distinct per concurrently-open NSP session
        # on this rig -- without it, two Sessions for the same device_type
        # (both LEGACY_NSP here) collide on the same shared-memory segment
        # name and the second one silently attaches as a client to the
        # first's buffers instead of opening its own (root-caused and fixed
        # upstream in pycbsdk; see CLAUDE.md). Each cerebusAdapterBCM node
        # in a multi-NSP graph needs its own shmem_instance value.
        session_kwargs = dict(device_type=DeviceType.LEGACY_NSP,
                               device_address=self.device_address,
                               shmem_instance=self.shmem_instance)
        if self.client_interface:
            session_kwargs['client_interface'] = self.client_interface

        logging.info(f'Connecting to NSP at {self.device_address} '
                     f'(client_interface={self.client_interface}, '
                     f'shmem_instance={self.shmem_instance})...')
        try:
            self.session = Session(**session_kwargs)
        except Exception:
            logging.exception('Failed to open cbsdk Session')
            sys.exit(1)

        # Session() itself returns as soon as the socket/background thread
        # are set up -- it does NOT block until the device handshake
        # completes. Querying channel/group config before that finishes
        # blocks indefinitely with no diagnostic (confirmed empirically:
        # against an unreachable device, get_group_channels() never
        # returns). Poll runlevel with an explicit timeout instead of
        # guessing a fixed sleep.
        deadline = time.monotonic() + self.connect_timeout_s
        while self.session.runlevel < RUNLEVEL_RUNNING:
            if time.monotonic() > deadline:
                logging.error(
                    f'NSP at {self.device_address} never reached RUNNING '
                    f'runlevel within {self.connect_timeout_s}s (stuck at '
                    f'runlevel {self.session.runlevel}) -- check '
                    f'device_address/client_interface and that the NSP is '
                    f'powered on and broadcasting.')
                sys.exit(1)
            time.sleep(0.1)
        logging.info(f'NSP at {self.device_address} reached RUNNING runlevel.')

        # Discover which Sample Groups are actually active (non-empty
        # channel list) right now, and which channels/labels are in each --
        # this is the live equivalent of parse_ccf.py / poll_nsp_config.py,
        # done once at startup rather than baked into the graph YAML.
        for group_id in range(1, 7):
            channel_ids = self.session.get_group_channels(group_id)
            if not channel_ids:
                continue

            labels = [self.session.get_channel_label(ch) for ch in channel_ids]
            rate_hz = SampleRate(group_id).hz
            n_channels = len(channel_ids)

            info = _GroupInfo(group_id, rate_hz, n_channels, channel_ids, labels)
            self.groups[group_id] = info

            logging.info(f'Discovered Sample Group {group_id} ({rate_hz} Hz): '
                         f'{n_channels} channels (per get_group_channels), '
                         f'channel_id range [{min(channel_ids)}, {max(channel_ids)}]')

            # Do NOT classify/write the channel_map yet. Confirmed
            # empirically that get_group_channels()'s reported count can
            # differ from the actual delivered batch width -- e.g. one
            # group here reports 271 channels but every real batch has 272
            # (an extra zero-padded channel Blackrock's protocol apparently
            # includes beyond the labeled list -- same pattern already
            # documented in this repo's t12_graspcomplex_journeymode.yaml:
            # "N_ANALOG_CHANNELS: 4  # set to N+1, because analog packets
            # include an extra channel with padded zeros"). Calibrate (and
            # classify neural-vs-analog columns) from the first real batch
            # instead of trusting the discovery-time count.

            # on_group_batch collects however many packets accumulated in
            # one internal queue drain (~1ms/~30 packets worth at 30kHz,
            # per pycbsdk's own docstring) and calls back once per drain
            # with a contiguous (n_samples, n_channels) array + matching
            # (n_samples,) timestamps -- far less Python-callback overhead
            # than a callback per raw packet, and gives us real per-sample
            # device timestamps (unlike Session.continuous_reader, which
            # only accumulates samples, no timestamps).
            self.session.on_group_batch(group_id)(self._make_batch_callback(info))

        if not self.groups:
            logging.error('No active Sample Groups found on this NSP -- '
                          'nothing to stream. Check the live Central config.')
            sys.exit(1)

    def _classify_channels(self, info):
        # Split this group's (possibly calibration-adjusted) channel_ids
        # into neural vs. analog column indices, using the real Blackrock
        # channel_id -- not column position or group_id -- so this stays
        # correct regardless of how Central buckets channels into groups.
        # Padding columns (channel_id -1, appended by _calibrate when the
        # real batch is wider than get_group_channels() reported) fall
        # through to "analog" here since they're not real neural data
        # either way and this avoids polluting the neural stream with them.
        neural_cols = [i for i, cid in enumerate(info.channel_ids)
                       if 1 <= cid <= self.n_neural_channels]
        analog_cols = [i for i, cid in enumerate(info.channel_ids)
                       if cid > self.n_neural_channels]
        info.neural_cols = np.array(neural_cols, dtype=np.intp)
        info.analog_cols = np.array(analog_cols, dtype=np.intp)
        logging.info(f'Group {info.group_id}: classified {len(neural_cols)} channels '
                     f'-> "{self.neural_stream_name}", {len(analog_cols)} channels '
                     f'-> "{self.analog_stream_name}" (n_neural_channels={self.n_neural_channels})')

    def _write_channel_map(self, info):
        # One row per destination stream this group actually feeds, with
        # only that destination's channel_ids/labels -- so downstream
        # consumers don't have to guess (see the CLAUDE.md gotcha on
        # cbpkt_header.chid always being zero in raw packets -- cbsdk's
        # get_group_channels() is what actually resolves this).
        channel_ids = np.array(info.channel_ids, dtype=np.int32)
        labels = np.array(info.labels, dtype=object)
        for stream_name, cols in ((self.neural_stream_name, info.neural_cols),
                                   (self.analog_stream_name, info.analog_cols)):
            if cols.size == 0:
                continue
            self.r.xadd(self.channel_map_stream, {
                'group': info.group_id,
                'rate_hz': info.rate_hz,
                'stream_name': stream_name,
                'n_channels': int(cols.size),
                'channel_ids': channel_ids[cols].tobytes(),
                'labels': json.dumps(list(labels[cols])),
            })

    def _calibrate(self, info, actual_n_channels):
        # Called once, from inside the first batch callback for this group,
        # holding info.lock. Reconciles get_group_channels()'s reported
        # count against what's actually arriving, classifies neural vs.
        # analog columns, and only THEN writes the channel_map -- see the
        # comment in connect_and_discover for why the discovery-time count
        # isn't trusted as-is.
        expected = info.n_channels
        if actual_n_channels != expected:
            logging.warning(
                f'Group {info.group_id}: get_group_channels() reported '
                f'{expected} channels but the first real batch has '
                f'{actual_n_channels}. Using the actual batch width for '
                f'the channel_map. If this is more than a small padding '
                f'difference, treat it as suspect -- on this rig, a '
                f'larger-than-expected mismatch has meant another NSP\'s '
                f'data was leaking into this session (see client_interface '
                f'warning in parameter_initialization).')
            if actual_n_channels > expected:
                pad = actual_n_channels - expected
                info.channel_ids = list(info.channel_ids) + [-1] * pad
                info.labels = list(info.labels) + [f'unknown_pad_{i}' for i in range(pad)]
            else:
                info.channel_ids = list(info.channel_ids)[:actual_n_channels]
                info.labels = list(info.labels)[:actual_n_channels]
        info.n_channels = actual_n_channels
        self._classify_channels(info)
        self._write_channel_map(info)
        info.calibrated = True

    def _emit_entry(self, info, samples_chunk, timestamps_chunk):
        # samples_chunk is a (n_channels, samples_per_entry) slice out of
        # info.pending_samples. Route the neural/analog column subsets to
        # their own streams -- a group with only one of the two types
        # present (e.g. Central reconfigured back to separate groups) just
        # writes to that one stream, since the other column-index array is
        # empty in that case.
        for stream_name, cols in ((self.neural_stream_name, info.neural_cols),
                                   (self.analog_stream_name, info.analog_cols)):
            if cols.size == 0:
                continue
            entry = {
                'samples': samples_chunk[cols, :].tobytes(),
                'timestamps': timestamps_chunk.tobytes(),
                'tracking_id': np.uint64(info.tracking_id).tobytes(),
                # Traceability: which Sample Group this entry actually came
                # from. Not read by any downstream node -- extra fields
                # pass through re_reference.py untouched -- purely for
                # answering "which group is this" later without
                # re-deriving it from the channel_map stream.
                'group_id': np.int32(info.group_id).tobytes(),
            }
            try:
                self.r.xadd(stream_name, entry)
            except Exception:
                logging.exception(f'Failed to XADD to {stream_name}')
        info.tracking_id += 1

    def _make_batch_callback(self, info):
        def on_batch(samples, timestamps):
            # Called from a pycbsdk-internal thread. `samples` arrives as
            # (n_samples, n_channels) int16 -- sample-major. But this
            # graph's downstream (re_reference.py / bpExtraction.py, from
            # brand-nsp) reshapes `samples` as (chan_per_stream,
            # samp_per_stream) -- i.e. it expects channel-major, matching
            # cerebusAdapterGem.c's per-channel-strided write convention,
            # NOT featureExtraction_and_binning.py's sample-major
            # convention (brand-east; a different downstream, not used in
            # this graph). Transpose before serializing so the flattened
            # byte layout matches what re_reference.py's reshape expects.
            # .tobytes() on a transposed view already serializes in the
            # transposed (logical) order, not the original memory order --
            # verified empirically -- so ascontiguousarray isn't required
            # for correctness here. Kept anyway for a real contiguous copy
            # to serialize, mainly so this doesn't rely on that tobytes()
            # behavior if this array is ever passed to something less
            # forgiving of non-contiguous views.
            n_samples, n_channels = samples.shape
            samples_cm = np.ascontiguousarray(samples.T)  # (n_channels, n_samples)
            timestamps = timestamps.astype(np.uint64)

            if not info.calibrated:
                with info.lock:
                    if not info.calibrated:  # re-check inside the lock
                        self._calibrate(info, n_channels)
            elif n_channels != info.n_channels:
                # Recurring mismatch after calibration -- this is the
                # cross-NSP-contamination pattern, not a one-time padding
                # quirk. Drop the batch rather than write inconsistent
                # data under this group's established schema.
                logging.error(
                    f'Group {info.group_id}: batch has {n_channels} '
                    f'channels, expected {info.n_channels} (calibrated). '
                    f'Dropping this batch -- likely another NSP\'s data '
                    f'leaking in; check client_interface.')
                return

            # pycbsdk's batch width doesn't line up with the fixed
            # samples_per_entry downstream expects, so accumulate into a
            # per-group leftover buffer (protected by info.lock, since a
            # session can in principle deliver batches for the same group
            # from more than one internal thread) and emit exactly
            # samples_per_entry-wide entries, carrying any remainder over
            # to the next callback.
            with info.lock:
                if info.pending_samples is None:
                    info.pending_samples = samples_cm
                    info.pending_timestamps = timestamps
                else:
                    info.pending_samples = np.concatenate(
                        [info.pending_samples, samples_cm], axis=1)
                    info.pending_timestamps = np.concatenate(
                        [info.pending_timestamps, timestamps])

                while info.pending_samples.shape[1] >= self.samples_per_entry:
                    chunk = info.pending_samples[:, :self.samples_per_entry]
                    chunk_ts = info.pending_timestamps[:self.samples_per_entry]
                    info.pending_samples = info.pending_samples[:, self.samples_per_entry:]
                    info.pending_timestamps = info.pending_timestamps[self.samples_per_entry:]
                    self._emit_entry(info, chunk, chunk_ts)

        return on_batch

    def run(self):
        logging.info(f'Streaming {len(self.groups)} Sample Group(s) '
                     f'from {self.device_address} -- neural to '
                     f'"{self.neural_stream_name}", analog to '
                     f'"{self.analog_stream_name}". Entering idle loop '
                     f'(data delivery happens via cbsdk callbacks).')
        while True:
            time.sleep(1)

    def terminate(self, *_):
        logging.info('SIGINT received, closing cbsdk session and exiting')
        if self.session is not None:
            try:
                self.session.close()
            except Exception:
                logging.exception('Error closing cbsdk session')
        gc.collect()
        sys.exit(0)


class _GroupInfo:
    def __init__(self, group_id, rate_hz, n_channels, channel_ids, labels):
        self.group_id = group_id
        self.rate_hz = rate_hz
        self.n_channels = n_channels
        self.channel_ids = channel_ids
        self.labels = labels
        # Set by _classify_channels() (called from _calibrate(), after the
        # real batch width is known) -- column-index arrays into this
        # group's (n_channels, samples_per_entry) buffer, one per
        # destination stream. Empty (not None) until then.
        self.neural_cols = np.array([], dtype=np.intp)
        self.analog_cols = np.array([], dtype=np.intp)
        self.tracking_id = 0
        self.calibrated = False
        self.lock = threading.Lock()
        # Leftover samples/timestamps not yet forming a full
        # samples_per_entry-wide chunk; carried over between on_batch
        # calls. (n_channels, n_pending) / (n_pending,), or None before
        # the first batch arrives.
        self.pending_samples = None
        self.pending_timestamps = None


if __name__ == '__main__':
    gc.disable()
    node = CerebusAdapterBCM()
    node.run()
    gc.collect()
