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
            stream_name = f'{self.redis_output_prefix}_g{group_id}'

            info = _GroupInfo(group_id, rate_hz, stream_name, n_channels, channel_ids, labels)
            self.groups[group_id] = info

            logging.info(f'Discovered Sample Group {group_id} ({rate_hz} Hz): '
                         f'{n_channels} channels (per get_group_channels) -> '
                         f'stream "{stream_name}"')

            # Do NOT write the channel_map yet. Confirmed empirically that
            # get_group_channels()'s reported count can differ from the
            # actual delivered batch width -- e.g. group 3 here reports 165
            # channels but every real batch has 166 (an extra
            # zero-padded channel Blackrock's protocol apparently includes
            # beyond the labeled list -- same pattern already documented in
            # this repo's t12_graspcomplex_journeymode.yaml:
            # "N_ANALOG_CHANNELS: 4  # set to N+1, because analog packets
            # include an extra channel with padded zeros"). Calibrate from
            # the first real batch instead of trusting the discovery-time
            # count, and write the channel_map then.

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

    def _write_channel_map(self, info):
        # One-time metadata entry per active group: the exact channel
        # order used for this stream's `samples` buffer columns, so
        # downstream consumers don't have to guess (see the CLAUDE.md
        # gotcha on cbpkt_header.chid always being zero in raw packets --
        # cbsdk's get_group_channels() is what actually resolves this).
        self.r.xadd(self.channel_map_stream, {
            'group': info.group_id,
            'rate_hz': info.rate_hz,
            'stream_name': info.stream_name,
            'n_channels': info.n_channels,
            'channel_ids': np.array(info.channel_ids, dtype=np.int32).tobytes(),
            'labels': json.dumps(info.labels),
        })

    def _calibrate(self, info, actual_n_channels):
        # Called once, from inside the first batch callback for this group,
        # holding info.lock. Reconciles get_group_channels()'s reported
        # count against what's actually arriving, and only THEN writes the
        # channel_map -- see the comment in connect_and_discover for why
        # the discovery-time count isn't trusted as-is.
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
        self._write_channel_map(info)
        info.calibrated = True

    def _make_batch_callback(self, info):
        def on_batch(samples, timestamps):
            # Called from a pycbsdk-internal thread. `samples` is already
            # (n_samples, n_channels) int16 -- sample-major, matching the
            # legacy cerebusAdapter.c convention documented in CLAUDE.md,
            # so this stream stays compatible with existing downstream
            # nodes (e.g. featureExtraction_and_binning.py's
            # np.reshape(..., (n_samples, n_channels))). No reshaping needed.
            n_samples, n_channels = samples.shape

            if not info.calibrated:
                with info.lock:
                    if not info.calibrated:  # re-check inside the lock
                        self._calibrate(info, n_channels)
            elif n_channels != info.n_channels:
                # Recurring mismatch after calibration -- this is the
                # cross-NSP-contamination pattern, not a one-time padding
                # quirk. Drop the batch rather than write inconsistent
                # data under this stream's established schema.
                logging.error(
                    f'Group {info.group_id} on {info.stream_name}: batch '
                    f'has {n_channels} channels, expected {info.n_channels} '
                    f'(calibrated). Dropping this batch -- likely another '
                    f'NSP\'s data leaking in; check client_interface.')
                return

            entry = {
                'samples': samples.tobytes(),
                'timestamps': timestamps.astype(np.uint64).tobytes(),
                'tracking_id': np.uint64(info.tracking_id).tobytes(),
            }
            try:
                self.r.xadd(info.stream_name, entry)
            except Exception:
                logging.exception(f'Failed to XADD to {info.stream_name}')
            info.tracking_id += 1

        return on_batch

    def run(self):
        logging.info(f'Streaming {len(self.groups)} Sample Group(s) '
                     f'from {self.device_address}. Entering idle loop '
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
    def __init__(self, group_id, rate_hz, stream_name, n_channels, channel_ids, labels):
        self.group_id = group_id
        self.rate_hz = rate_hz
        self.stream_name = stream_name
        self.n_channels = n_channels
        self.channel_ids = channel_ids
        self.labels = labels
        self.tracking_id = 0
        self.calibrated = False
        self.lock = threading.Lock()


if __name__ == '__main__':
    gc.disable()
    node = CerebusAdapterBCM()
    node.run()
    gc.collect()
