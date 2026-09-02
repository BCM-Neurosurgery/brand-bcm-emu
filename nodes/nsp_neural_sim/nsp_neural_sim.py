#!/usr/bin/env python
# nsp_neural_sim.py
#
# Shim node for testing the mediaPlayerTask.yaml pipeline with simulated
# neural data instead of a real NSP ("Option B" -- see CLAUDE.md discussion
# with Eric). Sits between brand-simulator's spike_gen_30k and re_reference,
# reformatting spike_gen_30k's output into exactly what re_reference (and,
# transitively, bpExtraction/thresholdExtraction/bin_multiple) expect from a
# real cerebusAdapterBCM node. Confirmed by reading re_reference.py directly
# that only 'samples' + 'timestamps' are required there -- but bpExtraction/
# thresholdExtraction have their own additional requirement, found only
# after they crashed with `KeyError: b'tracking_id'` on the first live run:
# with use_tracking_id: True (set in this graph), they read a per-entry
# 'tracking_id' scalar straight out of the (passed-through-unchanged-by-
# re_reference) entry_data. Real cerebusAdapterBCM writes one incrementing
# uint64 tracking_id per entry; synthesized here the same way.
#
# Three reformats are needed:
#   1. spike_gen_30k's 'continuous' field is sample-major
#      (samp_per_stream, n_channels); re_reference reshapes 'samples' as
#      channel-major (chan_per_stream, samp_per_stream). Must transpose.
#   2. spike_gen_30k has no per-sample timestamp field at all (only a
#      per-entry monotonic 'ts' float) -- re_reference requires a
#      'timestamps' array it checks for monotonicity. Synthesized here as a
#      simple running uint64 sample counter.
#   3. spike_gen_30k has no tracking_id field at all -- bpExtraction/
#      thresholdExtraction require one when use_tracking_id: True.
#      Synthesized here as a running per-entry uint64 counter, matching
#      cerebusAdapterBCM's own semantics (one increment per entry, not per
#      sample).
#
# This is throwaway/testing-only code -- it does not carry over if the graph
# later switches to feeding simulated data through a real raw-socket NSP
# adapter node (cerebusAdapterGem), which produces this shape natively.

import gc
import logging
import signal

import numpy as np
from brand import BRANDNode


class NSPNeuralSim(BRANDNode):

    def __init__(self):
        super().__init__()

        self.input_stream = self.parameters['input_stream']
        self.output_stream = self.parameters.get('output_stream', 'nsp_neural')
        self.n_channels = self.parameters['n_channels']
        self.samp_per_stream = self.parameters.get('samp_per_stream', 30)
        self.maxlen = self.parameters.setdefault('maxlen', 2000)

        self.sample_counter = np.uint64(0)
        self.tracking_id = np.uint64(0)
        self.input_id = '$'

        signal.signal(signal.SIGINT, self.terminate)

        logging.info(
            f'Reading simulated continuous data from {self.input_stream}, '
            f'writing {self.n_channels}ch x {self.samp_per_stream} samples '
            f'to {self.output_stream}')

    def run(self):
        while True:
            replies = self.r.xread({self.input_stream: self.input_id},
                                    block=0, count=1)

            self.input_id = replies[0][1][0][0]
            entry_data = replies[0][1][0][1]

            # spike_gen_30k writes 'continuous' as (samp_per_stream,
            # n_channels) int16, sample-major. Transpose to the
            # channel-major (n_channels, samp_per_stream) layout
            # re_reference expects.
            continuous = np.frombuffer(
                entry_data[b'continuous'], dtype=np.int16
            ).reshape((self.samp_per_stream, self.n_channels))
            samples = np.ascontiguousarray(continuous.T)

            timestamps = (self.sample_counter +
                          np.arange(self.samp_per_stream, dtype=np.uint64))
            self.sample_counter += np.uint64(self.samp_per_stream)

            self.r.xadd(
                self.output_stream, {
                    'samples': samples.tobytes(),
                    'timestamps': timestamps.tobytes(),
                    'tracking_id': self.tracking_id.tobytes(),
                },
                maxlen=self.maxlen,
                approximate=True)
            self.tracking_id += np.uint64(1)


if __name__ == '__main__':
    gc.disable()

    nsp_neural_sim = NSPNeuralSim()
    nsp_neural_sim.run()

    gc.collect()
