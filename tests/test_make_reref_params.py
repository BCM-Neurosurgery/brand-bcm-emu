import os
import sys

import numpy as np
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))
import make_reref_params as mrp


# --------------------------------------------------------------------------- #
# CAR matrix construction
# --------------------------------------------------------------------------- #

def test_car_matrix_is_block_diagonal_with_right_block_size():
    chan_total, channels_per_array = 512, 128
    S = mrp.build_car_matrix(chan_total, channels_per_array)

    assert S.dtype == np.float64
    assert S.shape == (chan_total, chan_total)

    n_arrays = chan_total // channels_per_array
    for a in range(n_arrays):
        start = a * channels_per_array
        end = start + channels_per_array
        block = S[start:end, start:end]
        assert np.allclose(block, 1.0 / channels_per_array)

        # everything outside this array's own block, in both the row and
        # column direction, must be exactly zero
        row_outside = np.delete(S[start:end, :], np.s_[start:end], axis=1)
        col_outside = np.delete(S[:, start:end], np.s_[start:end], axis=0)
        assert np.all(row_outside == 0.0)
        assert np.all(col_outside == 0.0)


def test_car_matrix_zeros_a_constant_per_array_signal():
    chan_total, channels_per_array = 256, 64
    S = mrp.build_car_matrix(chan_total, channels_per_array)

    # a signal that's constant within each array (but differs across
    # arrays) should be entirely removed by (I - S)
    n_arrays = chan_total // channels_per_array
    rng = np.random.default_rng(0)
    per_array_values = rng.normal(size=n_arrays)
    signal = np.repeat(per_array_values, channels_per_array)

    referenced = (np.eye(chan_total) - S) @ signal
    assert np.allclose(referenced, 0.0, atol=1e-10)


def test_car_matrix_rejects_non_divisible_channel_count():
    with pytest.raises(ValueError):
        mrp.build_car_matrix(300, 128)


# --------------------------------------------------------------------------- #
# Unshuffling / validation
# --------------------------------------------------------------------------- #

def test_identity_unshuffling_passes_validation():
    chan_total = 512
    U = mrp.build_identity_unshuffling(chan_total)
    hub_ranges = mrp.hub_ranges_from_sizes([256, 256])

    assert U.dtype == np.float64
    assert U.shape == (chan_total, chan_total)
    mrp.validate_block_closure(U, hub_ranges)  # should not raise


def test_within_hub_permutation_passes_validation():
    chan_total = 512
    hub_ranges = mrp.hub_ranges_from_sizes([256, 256])

    # 1-indexed permutation that only shuffles within each 256-channel hub
    rng = np.random.default_rng(1)
    mapping = np.empty(chan_total, dtype=np.int64)
    for start, size in hub_ranges:
        local = rng.permutation(size)
        mapping[start:start + size] = start + local + 1  # back to 1-indexed

    U = mrp.build_unshuffling_from_map(mapping, chan_total)
    mrp.validate_block_closure(U, hub_ranges)  # should not raise


def test_cross_hub_permutation_is_caught():
    chan_total = 512
    hub_ranges = mrp.hub_ranges_from_sizes([256, 256])

    # identity, except channel 0 (hub 1) and channel 256 (hub 2) are swapped
    mapping = np.arange(1, chan_total + 1, dtype=np.int64)
    mapping[0], mapping[256] = mapping[256], mapping[0]

    U = mrp.build_unshuffling_from_map(mapping, chan_total)
    with pytest.raises(ValueError, match=r'not closed'):
        mrp.validate_block_closure(U, hub_ranges)


def test_electrode_mapping_round_trip_dtype_and_shape(tmp_path):
    import json

    chan_total = 256
    rng = np.random.default_rng(2)
    mapping = rng.permutation(chan_total) + 1  # 1-indexed

    path = tmp_path / 'electrode_map.json'
    path.write_text(json.dumps({'electrode_mapping': mapping.tolist()}))

    loaded = mrp.load_electrode_mapping(str(path), chan_total)
    assert np.array_equal(loaded, mapping)

    U = mrp.build_unshuffling_from_map(loaded, chan_total)
    assert U.dtype == np.float64
    assert U.shape == (chan_total, chan_total)
    # every row and column of a permutation matrix sums to exactly 1
    assert np.allclose(U.sum(axis=0), 1.0)
    assert np.allclose(U.sum(axis=1), 1.0)


def test_electrode_mapping_rejects_wrong_length(tmp_path):
    import json

    path = tmp_path / 'electrode_map.json'
    path.write_text(json.dumps({'electrode_mapping': [1, 2, 3]}))

    with pytest.raises(ValueError):
        mrp.load_electrode_mapping(str(path), 256)


def test_electrode_mapping_rejects_invalid_permutation(tmp_path):
    import json

    path = tmp_path / 'electrode_map.json'
    # duplicate entry, not a valid permutation of 1..4
    path.write_text(json.dumps({'electrode_mapping': [1, 2, 2, 4]}))

    with pytest.raises(ValueError):
        mrp.load_electrode_mapping(str(path), 4)


def test_hub_ranges_from_sizes():
    assert mrp.hub_ranges_from_sizes([256]) == [(0, 256)]
    assert mrp.hub_ranges_from_sizes([256, 256]) == [(0, 256), (256, 256)]
    assert mrp.hub_ranges_from_sizes([96, 96, 64]) == [(0, 96), (96, 96), (192, 64)]
