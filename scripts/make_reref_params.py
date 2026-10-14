#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""make_reref_params.py

Builds and writes the `rereference_parameters` Redis stream entry that
brand-nsp's `re_reference` node reads at startup
(../brand-nsp/nodes/re_reference/re_reference.py, `initialize_coefficients()`).

Lives here, not in brand-nsp, even though it's about a brand-nsp node:
this script is BCM-site configuration (our specific 256/512-channel hub
layout), not a generic reusable processing node -- brand-nsp is the
shared, multi-site node library (see its own README/branch list), while
this repo is where BCM-specific graphs and setup tooling live. It has no
Python import dependency on brand-nsp (pure argparse/json/logging/numpy);
its only link to that repo is the Redis wire contract described below.

Why this is a standalone script, not a node or a derivative
-------------------------------------------------------------------------
Nothing in brand-nsp has ever written to `rereference_parameters`, so
`re_reference` has always silently fallen through to its fallback path: a
plain global mean subtraction across all channels, ignoring array
boundaries. This script exists to write that stream so the node takes its
real path instead -- per-array common average referencing (CAR), with an
optional channel unshuffling step.

It is intentionally *not* supervisor-managed: it is not registered under
`nodes/` or `derivatives/`, appears in no graph YAML, has no lifecycle, and
is not CPU-pinned. It connects to Redis, writes exactly one stream entry,
and exits.

This is possible because the matrices below depend only on which channel
belongs to which array -- a fact fixed for a given participant -- and never
on live neural data. Contrast this with Stanford's equivalent,
`parameterUpdater.py` (brand-east), which genuinely IS a derivative: it
fits linear-regression-referencing (LRR) weights from a diagnostic block's
*data* each session. We are doing plain per-array CAR here, which needs no
data and no session-specific fitting, so it doesn't need a lifecycle either.

The Redis contract (verified against brand-nsp commit c4d4ab4,
nodes/re_reference/re_reference.py -- re-verify against that file if
brand-nsp has moved on since)
-------------------------------------------------------------------------
One entry on the `rereference_parameters` stream (or whatever
`coefs_stream_name` a graph configures), with two fields:

    channel_scaling      float64, shape (chan_total, chan_total), required
    channel_unshuffling  float64, shape (chan_total, chan_total), optional
                          (re_reference.py defaults to identity if absent)

float64 is not negotiable -- `np.frombuffer(..., dtype=np.float64)` in the
node is hardcoded. `re_reference.py` reads only the single most recent
entry (`xrevrange(..., count=1)`), so one `XADD` is sufficient; this script
always writes both fields explicitly rather than relying on the "absent
means identity" default, so a stream dump is unambiguous about what ran.

The node computes `(I - channel_scaling) @ channel_unshuffling` and applies
that to incoming data, so unshuffling happens first: channels are
reordered into array order, and referencing happens in that reordered
space.

The cross-hub-leakage risk
-------------------------------------------------------------------------
`re_reference` slices BOTH matrices as a square diagonal block, using a
per-node `start_channel`/`chan_per_stream` pair (one node instance per hub
in a dual-hub graph). Any coefficient that falls outside a hub's own
diagonal block is silently discarded -- if a permutation or a CAR grouping
ever assigns a channel across a hub boundary, that channel's row quietly
zeroes out downstream, with no error anywhere. `validate_block_closure()`
below exists specifically to catch this before anything is written to
Redis.
"""
import argparse
import json
import logging

import numpy as np

DEFAULT_STREAM_NAME = 'rereference_parameters'
DEFAULT_REDIS_HOST = '127.0.0.1'
DEFAULT_REDIS_PORT = 6379


# --------------------------------------------------------------------------- #
# Matrix construction -- pure functions, no Redis. Importable and testable on
# any machine with numpy, no Linux and no Redis required.
# --------------------------------------------------------------------------- #

def build_car_matrix(chan_total: int, channels_per_array: int) -> np.ndarray:
    """Build the per-array common-average-reference scaling matrix.

    S[i, j] = 1 / channels_per_array if channels i and j are in the same
    array, else 0. Block-diagonal, one dense block per array, each block
    channels_per_array x channels_per_array. `(I - S)` (computed by the
    node, not here) then subtracts each channel's own array mean.

    Parameters
    ----------
    chan_total : int
        Total number of channels this matrix spans (e.g. 512 for a
        two-hub, 256-channels-per-hub graph).
    channels_per_array : int
        Number of channels per electrode array. Must evenly divide
        chan_total.

    Returns
    -------
    np.ndarray
        float64, shape (chan_total, chan_total).
    """
    if chan_total <= 0 or channels_per_array <= 0:
        raise ValueError('chan_total and channels_per_array must be positive')
    if chan_total % channels_per_array != 0:
        raise ValueError(
            f'chan_total ({chan_total}) is not evenly divisible by '
            f'channels_per_array ({channels_per_array})')

    n_arrays = chan_total // channels_per_array
    S = np.zeros((chan_total, chan_total), dtype=np.float64)
    for a in range(n_arrays):
        start = a * channels_per_array
        end = start + channels_per_array
        S[start:end, start:end] = 1.0 / channels_per_array
    return S


def build_identity_unshuffling(chan_total: int) -> np.ndarray:
    """Identity channel-unshuffling matrix (no reordering)."""
    return np.eye(chan_total, dtype=np.float64)


def load_electrode_mapping(path: str, chan_total: int) -> np.ndarray:
    """Load a participant's electrode mapping JSON.

    Expected format (matches brand-east's NeuralFeatureExtractor.py /
    `order_channels_using_mapping` convention):
    `{"electrode_mapping": [...]}`, a 1-indexed permutation of
    `1..chan_total`, length chan_total, where output position k should come
    from hardware channel `electrode_mapping[k] - 1` (0-indexed).

    Raises
    ------
    ValueError
        If the file's length doesn't match chan_total, or its contents
        aren't a valid 1-indexed permutation of 1..chan_total.
    """
    with open(path, 'r') as f:
        data = json.load(f)
    mapping = np.asarray(data['electrode_mapping'], dtype=np.int64)
    if mapping.shape != (chan_total,):
        raise ValueError(
            f'electrode_mapping in {path} has length {mapping.shape[0]}, '
            f'expected chan_total ({chan_total})')
    if sorted(mapping.tolist()) != list(range(1, chan_total + 1)):
        raise ValueError(
            f'electrode_mapping in {path} is not a 1-indexed permutation '
            f'of 1..{chan_total}')
    return mapping


def build_unshuffling_from_map(mapping: np.ndarray, chan_total: int) -> np.ndarray:
    """Build a permutation matrix from a 1-indexed electrode mapping.

    Matches brand-east's `order_channels_using_mapping`: output position k
    comes from hardware channel `mapping[k] - 1` (0-indexed), i.e.
    `U[k, mapping[k] - 1] = 1`.

    Parameters
    ----------
    mapping : np.ndarray
        1-indexed permutation of 1..chan_total, length chan_total (see
        `load_electrode_mapping`).
    chan_total : int

    Returns
    -------
    np.ndarray
        float64, shape (chan_total, chan_total).
    """
    if mapping.shape != (chan_total,):
        raise ValueError(
            f'mapping length {mapping.shape[0]} != chan_total ({chan_total})')
    U = np.zeros((chan_total, chan_total), dtype=np.float64)
    idx0 = mapping - 1
    U[np.arange(chan_total), idx0] = 1.0
    return U


def hub_ranges_from_sizes(hub_sizes) -> list:
    """Convert a list of per-hub channel counts into (start, size) ranges.

    Mirrors re_reference's own convention: hub i owns the contiguous slice
    `[start_channel, start_channel + chan_per_stream)`, with hubs
    concatenated in the order given (hub 0 starts at channel 0, hub 1
    starts where hub 0 ends, etc).
    """
    ranges = []
    start = 0
    for size in hub_sizes:
        if size <= 0:
            raise ValueError(f'hub size must be positive, got {size}')
        ranges.append((start, size))
        start += size
    return ranges


def validate_block_closure(matrix: np.ndarray, hub_ranges, rtol: float = 1e-8,
                            atol: float = 1e-8) -> None:
    """Check that each hub's diagonal block is "closed" under `matrix`.

    For each (start, size) hub range, extracts the diagonal block
    `matrix[start:start+size, start:start+size]` and requires every row sum
    and every column sum within that block to equal 1. A row sum != 1 means
    that channel's coefficient mass leaks out of its own hub's block (its
    row has nonzero weight on a channel actually belonging to another hub);
    a column sum != 1 means some other hub's channel has nonzero weight
    landing inside this block. Either case is the silent-dead-channel bug
    this function exists to catch -- re_reference only ever reads the
    diagonal block for a hub, so any mass outside it is discarded with no
    error at runtime.

    This check applies equally to a per-array CAR matrix (whose blocks
    should already sit entirely within one hub, if arrays are defined
    sanely) and to a channel-unshuffling permutation matrix (whose
    permutation must not move any channel across a hub boundary).

    Raises
    ------
    ValueError
        Naming the specific channel indices whose row or column sum is not
        1, if any.
    """
    chan_total = matrix.shape[0]
    if matrix.shape != (chan_total, chan_total):
        raise ValueError(f'matrix must be square, got shape {matrix.shape}')

    for start, size in hub_ranges:
        end = start + size
        block = matrix[start:end, start:end]
        row_sums = block.sum(axis=1)
        col_sums = block.sum(axis=0)
        bad_rows = np.where(~np.isclose(row_sums, 1.0, rtol=rtol, atol=atol))[0]
        bad_cols = np.where(~np.isclose(col_sums, 1.0, rtol=rtol, atol=atol))[0]
        if bad_rows.size or bad_cols.size:
            bad_row_idx = [start + i for i in bad_rows.tolist()]
            bad_col_idx = [start + i for i in bad_cols.tolist()]
            raise ValueError(
                f'Hub block [{start}, {end}) is not closed -- coefficient '
                f'mass crosses the hub boundary. '
                f'Channels with row-sum leakage (out of this block): '
                f'{bad_row_idx}. '
                f'Channels with column-sum leakage (in from another '
                f'block): {bad_col_idx}.')


# --------------------------------------------------------------------------- #
# Redis I/O
# --------------------------------------------------------------------------- #

def write_to_redis(host: str, port: int, stream_name: str,
                    channel_scaling: np.ndarray,
                    channel_unshuffling: np.ndarray) -> None:
    import redis

    r = redis.Redis(host=host, port=port)
    entry = {
        'channel_scaling': channel_scaling.astype(np.float64).tobytes(),
        'channel_unshuffling': channel_unshuffling.astype(np.float64).tobytes(),
    }
    r.xadd(stream_name, entry)
    logging.info(f'Wrote rereference parameters to Redis stream '
                 f'"{stream_name}" at {host}:{port}')


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def parse_args():
    parser = argparse.ArgumentParser(
        description='Build and write the rereference_parameters Redis '
                    'stream entry consumed by the re_reference node.')
    parser.add_argument('--chan-total', type=int, required=True,
                        help='Total channel count this matrix spans '
                             '(sum of all hub sizes).')
    parser.add_argument('--channels-per-array', type=int, required=True,
                        help='Channels per electrode array, for the '
                             'per-array CAR matrix. Must evenly divide '
                             '--chan-total.')
    parser.add_argument('--hub-sizes', type=int, nargs='+', required=True,
                        help='Channel count per hub, in hub order (e.g. '
                             '"--hub-sizes 256" for a single-hub graph, '
                             '"--hub-sizes 256 256" for dual-hub). Must '
                             'sum to --chan-total. Defines the diagonal '
                             'blocks validated for cross-hub leakage.')
    parser.add_argument('--electrode-map', type=str, default=None,
                        help='Path to a participant electrode mapping '
                             'JSON ({"electrode_mapping": [...]}, '
                             '1-indexed, length chan_total). If omitted, '
                             'channel_unshuffling is the identity matrix.')
    parser.add_argument('--redis-host', type=str, default=DEFAULT_REDIS_HOST)
    parser.add_argument('--redis-port', type=int, default=DEFAULT_REDIS_PORT)
    parser.add_argument('--stream-name', type=str, default=DEFAULT_STREAM_NAME,
                        help='Must match the coefs_stream_name configured '
                             'on the re_reference node(s) in the graph '
                             f'YAML (default: "{DEFAULT_STREAM_NAME}").')
    parser.add_argument('--dry-run', action='store_true',
                        help='Build and validate the matrices and print a '
                             'summary, without connecting to Redis.')
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    args = parse_args()

    if sum(args.hub_sizes) != args.chan_total:
        raise ValueError(
            f'--hub-sizes {args.hub_sizes} sums to {sum(args.hub_sizes)}, '
            f'which does not match --chan-total ({args.chan_total})')

    hub_ranges = hub_ranges_from_sizes(args.hub_sizes)

    channel_scaling = build_car_matrix(args.chan_total, args.channels_per_array)

    if args.electrode_map is not None:
        mapping = load_electrode_mapping(args.electrode_map, args.chan_total)
        channel_unshuffling = build_unshuffling_from_map(mapping, args.chan_total)
        logging.info(f'Using electrode mapping from {args.electrode_map}')
    else:
        channel_unshuffling = build_identity_unshuffling(args.chan_total)
        logging.info('No --electrode-map given -- channel_unshuffling is '
                     'identity (channels assumed already in array order)')

    # Refuse to write on validation failure -- validate both matrices before
    # touching Redis.
    validate_block_closure(channel_scaling, hub_ranges)
    validate_block_closure(channel_unshuffling, hub_ranges)
    logging.info(f'Validated: all {len(hub_ranges)} hub block(s) are closed '
                 f'under both matrices (hub ranges: {hub_ranges})')

    if args.dry_run:
        print('--dry-run: matrices built and validated, not written to Redis.')
        print(f'  chan_total:          {args.chan_total}')
        print(f'  channels_per_array:  {args.channels_per_array}')
        print(f'  hub_ranges:          {hub_ranges}')
        print(f'  channel_scaling:     dtype={channel_scaling.dtype}, '
              f'shape={channel_scaling.shape}')
        print(f'  channel_unshuffling: dtype={channel_unshuffling.dtype}, '
              f'shape={channel_unshuffling.shape}, '
              f'identity={np.array_equal(channel_unshuffling, np.eye(args.chan_total))}')
        return

    write_to_redis(args.redis_host, args.redis_port, args.stream_name,
                   channel_scaling, channel_unshuffling)


if __name__ == '__main__':
    main()
