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
import time

import numpy as np
from brand import BRANDNode
from pycbsdk import ChanInfoField, ChannelType, DeviceType, SampleRate, Session


class CerebusAdapterBCM(BRANDNode):

    def __init__(self):
        super().__init__()
        self.session = None
        self.group_buffers = {}  # group_id -> _GroupBuffer
        self.parameter_initialization()
        self.connect_and_discover()
        signal.signal(signal.SIGINT, self.terminate)

    def parameter_initialization(self):
        self.device_address = self.parameters['device_address']
        self.client_interface = self.parameters.get('client_interface')
        self.redis_output_prefix = self.parameters.get('redis_output_prefix', self.NAME)
        self.batch_ms = float(self.parameters.get('batch_ms', 1.0))
        self.channel_map_stream = f'{self.redis_output_prefix}_channel_map'

    def connect_and_discover(self):
        session_kwargs = dict(device_type=DeviceType.LEGACY_NSP,
                               device_address=self.device_address)
        if self.client_interface:
            session_kwargs['client_interface'] = self.client_interface

        logging.info(f'Connecting to NSP at {self.device_address} '
                     f'(client_interface={self.client_interface})...')
        try:
            self.session = Session(**session_kwargs)
        except Exception:
            logging.exception('Failed to open cbsdk Session')
            sys.exit(1)

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
            batch_size = max(1, round(rate_hz * self.batch_ms / 1000))

            stream_name = f'{self.redis_output_prefix}_g{group_id}'
            buf = _GroupBuffer(group_id, stream_name, n_channels, batch_size,
                                channel_ids, labels)
            self.group_buffers[group_id] = buf

            logging.info(f'Discovered Sample Group {group_id} ({rate_hz} Hz): '
                         f'{n_channels} channels -> stream "{stream_name}", '
                         f'batch_size={batch_size}')

            self._write_channel_map(buf, rate_hz)

            # Register the callback last, so `buf` is fully set up before
            # data can start arriving for it.
            self.session.on_group(group_id, as_array=True)(
                self._make_group_callback(buf))

        if not self.group_buffers:
            logging.error('No active Sample Groups found on this NSP -- '
                          'nothing to stream. Check the live Central config.')
            sys.exit(1)

    def _write_channel_map(self, buf, rate_hz):
        # One-time metadata entry per active group: the exact channel
        # order used for this stream's `samples` buffer columns, so
        # downstream consumers don't have to guess (see the CLAUDE.md
        # gotcha on cbpkt_header.chid always being zero in raw packets --
        # cbsdk's get_group_channels() is what actually resolves this).
        self.r.xadd(self.channel_map_stream, {
            'group': buf.group_id,
            'rate_hz': rate_hz,
            'stream_name': buf.stream_name,
            'n_channels': buf.n_channels,
            'channel_ids': np.array(buf.channel_ids, dtype=np.int32).tobytes(),
            'labels': json.dumps(buf.labels),
        })

    def _make_group_callback(self, buf):
        def on_group_packet(header, samples):
            # Called from a pycbsdk-internal thread, once per received
            # packet (i.e. once per time-sample across this group's
            # channels). Keep this fast: no per-call allocation beyond
            # what numpy's own slicing needs.
            i = buf.count
            buf.sample_buf[i, :] = samples
            buf.ts_buf[i] = header.time
            buf.count += 1

            if buf.count == buf.batch_size:
                self._flush(buf)

        return on_group_packet

    def _flush(self, buf):
        entry = {
            'samples': buf.sample_buf.tobytes(),
            'timestamps': buf.ts_buf.tobytes(),
            'tracking_id': np.uint64(buf.tracking_id).tobytes(),
        }
        try:
            self.r.xadd(buf.stream_name, entry)
        except Exception:
            logging.exception(f'Failed to XADD to {buf.stream_name}')
        buf.tracking_id += 1
        buf.count = 0

    def run(self):
        logging.info(f'Streaming {len(self.group_buffers)} Sample Group(s) '
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


class _GroupBuffer:
    def __init__(self, group_id, stream_name, n_channels, batch_size, channel_ids, labels):
        self.group_id = group_id
        self.stream_name = stream_name
        self.n_channels = n_channels
        self.batch_size = batch_size
        self.channel_ids = channel_ids
        self.labels = labels
        # sample-major layout [batch_size, n_channels], matching the legacy
        # cerebusAdapter.c convention documented in CLAUDE.md -- keeps this
        # node's stream format compatible with existing downstream nodes
        # (e.g. featureExtraction_and_binning.py's np.reshape(..., (n_samples, n_channels))).
        self.sample_buf = np.zeros((batch_size, n_channels), dtype=np.int16)
        self.ts_buf = np.zeros(batch_size, dtype=np.uint64)
        self.count = 0
        self.tracking_id = 0


if __name__ == '__main__':
    gc.disable()
    node = CerebusAdapterBCM()
    node.run()
    gc.collect()
