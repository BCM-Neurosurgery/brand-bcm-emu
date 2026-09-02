---
noteId: "4a7945e0999311f19290751382f48d14"
tags: []

---

# cerebusAdapterBCM

NSP data-acquisition node built on `pycbsdk` (CereLink SDK bindings) instead
of hand-rolled raw-UDP packet parsing like `cerebusAdapter`/`cerebusAdapterGem`
(`brand-nsp`). Auto-discovers which Cerebus Sample Groups are active on
connect and streams each one to its own Redis stream, without
`packet_type`/`chan_per_stream`/`samp_freq`/`broadcast_ip`/`broadcast_device`
needing to be hand-specified in the graph YAML.

## Why this node exists instead of extending `cerebusAdapter`

The legacy `cerebusAdapter.c` (`brand-modules/rice-cart/brand-modules/brand-east/nodes/cerebusAdapter/`)
has no `broadcast_device`/`SO_BINDTODEVICE` support at all -- it binds to
`INADDR_ANY` and silently ignores those graph YAML params, which caused
real cross-NSP data corruption when both NSPs were run through it
simultaneously (see `CLAUDE.md`). Rather than patch that shared node, this
is a new node built on `pycbsdk`, which already has working per-NIC
isolation (`client_interface`) for this rig.

## Parameters (graph YAML)

| Parameter | Required | Description |
|---|---|---|
| `device_address` | yes | NSP's cbsdk device address (both NSPs on this rig share the same address, `192.168.137.128`; see `client_interface` below for how they're actually told apart). |
| `client_interface` | **effectively required** | NIC to bind the client socket to (e.g. `enp4s0`, `enp5s0f3`). Confirmed empirically: omitting this can let a *different* NSP's data leak into this session's streams for the same Sample Group. Always set it explicitly for both NSPs on this rig, even the one that looks like "the default route." |
| `shmem_instance` | **required whenever more than one NSP node runs concurrently** | Distinct small integer per concurrently-open session of the same `device_type` (e.g. `0` for NSP1, `1` for NSP2). Without it, two concurrent `LEGACY_NSP` sessions collide on the same shared-memory segment name and the second one silently mirrors the first's data instead of opening its own -- see "Verified" below. Sessions that are meant to share state (e.g. multiple hubs of one Gemini system) should keep the same value; genuinely separate NSPs need different values. |
| `redis_output_prefix` | no (default: node `nickname`) | Prefix for `<redis_output_prefix>_channel_map`. No longer affects per-group stream naming -- see `neural_stream_name`/`analog_stream_name` below. |
| `samples_per_entry` | no (default: `30`) | Fixed number of samples per Redis entry. `on_group_batch`'s callback width varies with pycbsdk's internal queue drain, not a fixed count -- this node buffers across callbacks and emits exactly `samples_per_entry`-wide entries so downstream nodes (`re_reference.py`/`bpExtraction.py`, which reshape assuming a fixed graph-YAML `samp_per_stream`) don't hit a shape mismatch. Set to match whatever `samp_per_stream` the downstream consumer expects. |
| `n_neural_channels` | no (default: `256`) | Boundary between neural and analog channels, in real Blackrock **channel_id** terms (not Sample Group id, not column position): channel_id `1..n_neural_channels` is neural, anything above that is analog. This is a hardware-fixed fact per site (front-end channels are always the lowest-numbered block, the analog-input bank always immediately after -- `cbproto.h`'s bank layout), independent of which Sample Group(s) Central happens to bucket them into. Change this one number for a site with a different neural channel count; everything else about the routing stays the same. |
| `neural_stream_name` / `analog_stream_name` | no (default: `nsp_neural` / `nsp_analog`) | Fixed output stream names. Every active channel across every discovered Sample Group is classified by channel_id (see `n_neural_channels`) and routed to one of these two streams -- **not** to a per-group stream name. This is deliberately immune to Central reassigning channels between Sample Groups (e.g. changing a channel's configured sampling rate moves it to a different group automatically) -- confirmed to happen on this rig: setting the front-end channels to 30kHz merged them into the same group as the already-30kHz analog-input bank, which a group_id-keyed naming scheme would silently break on. A group that turns out to be 100% one type just never writes to the other stream. |
| `connect_timeout_s` | no (default: `10.0`) | How long to wait for the NSP to reach `RUNNING` runlevel before giving up with a clear error, instead of hanging forever. |
| `log` | no (default: `INFO`) | Standard BRANDNode log level. |

## What it does on startup

1. Opens a `pycbsdk.Session(DeviceType.LEGACY_NSP, device_address=..., client_interface=...)`.
2. Waits (with `connect_timeout_s`) for the device to reach `RUNLEVEL_RUNNING`.
3. For each Cerebus Sample Group (1-6), queries `get_group_channels(gid)` --
   if non-empty, that group is "active." For each active group:
   - Registers `on_group_batch(gid)`, which buffers incoming batches (of
     whatever width pycbsdk's internal queue drain happens to deliver) and
     emits fixed `samples_per_entry`-wide entries. Fields: `samples`
     (int16, **channel-major** `[n_channels, samples_per_entry]` --
     transposed from pycbsdk's native sample-major layout to match
     `cerebusAdapterGem.c`'s convention, which is what this graph's
     downstream, `re_reference.py`/`bpExtraction.py` from `brand-nsp`,
     expects; NOT the same convention as `featureExtraction_and_binning.py`
     from `brand-east`, a different downstream not used in this graph),
     `timestamps` (uint64, one per sample), `tracking_id`, `group_id`
     (traceability back to the originating Sample Group -- not consumed by
     any downstream node).
   - **Calibrates channel count from the first real batch, not from
     `get_group_channels()`'s count.** These can legitimately differ by a
     small, fixed amount -- e.g. one padding channel Blackrock's protocol
     includes beyond the labeled list (same pattern as this repo's
     `t12_graspcomplex_journeymode.yaml`'s `N_ANALOG_CHANNELS: N+1`
     comment). A *large* mismatch after calibration (checked on every
     subsequent batch) instead indicates cross-NSP contamination --
     that batch is logged and dropped rather than written under a
     mismatched schema.
   - **Classifies each column by real channel_id against `n_neural_channels`**
     (after calibration, so the classification uses the actual batch
     width) and, per emitted entry, writes the matching neural columns to
     `neural_stream_name` and the matching analog columns to
     `analog_stream_name` -- each as its own `xadd`, sliced from the same
     underlying buffer. A group is not required to be purely one type or
     the other; a combined group (confirmed to happen on this rig) just
     produces two entries per emission instead of one.
   - Writes one entry per destination stream to `<redis_output_prefix>_channel_map`
     recording the calibrated channel count plus the exact channel
     ID/label order used for that stream's `samples` columns -- this is
     what resolves the "which column is which physical channel" ambiguity
     that raw UDP parsing can't (`cbpkt_header.chid` is always zero on the
     wire; cbsdk's `get_group_channels()` is what actually knows the
     order).

## Environment: `nsp_env`, not `rt`

`pycbsdk` requires Python >=3.9; `rt` (used by every other node in this
repo) is pinned to 3.8 for its Cython/gcc toolchain and can't install it.
`nsp_env` has `pycbsdk`, `redis`, `brand` (editable installs), `pyyaml`,
`coloredlogs`, `sh`, `numpy`, and `cython` all installed for this purpose.
Build the same way as any other Python BRAND node, just with `nsp_env`
active instead of `rt`:
```bash
conda activate nsp_env
cd BRAND/BRAND && make ../brand-modules/brand-bcm-emu/nodes/cerebusAdapterBCM
```

## Verified end-to-end, concurrent dual-NSP, with real audio

Built and run against both NSPs on this rig's live NSP emulator
(`device_address=192.168.137.128`, `client_interface=enp4s0`/`shmem_instance=0`
for NSP1, `client_interface=enp5s0f3`/`shmem_instance=1` for NSP2):

- **Single-NSP, one at a time**: NSP1 discovered 2 active groups (Sample
  Group 3 @ 2kHz, 166 channels after calibration; Sample Group 5 @ 30kHz,
  12 channels) -- ~14,000 entries checked across both streams, zero
  malformed. NSP2 discovered 1 active group (Sample Group 5 @ 30kHz, 94
  channels -- neural + analog-input bank combined, columns 0-79 neural,
  80-93 analog-input in `get_group_channels()`'s order) -- ~22,700 entries
  checked, zero malformed. Also confirmed the `client_interface` cross-NSP
  leak empirically: connecting NSP1 *without* `client_interface`
  intermittently produced 94-channel batches (NSP2's channel count) on
  NSP1's group-5 stream.
- **Concurrent dual-NSP, via `graphs/dual_nsp_audio.yaml` through a real
  `supervisor` process** (both `cerebusAdapterBCM.bin` processes running
  at once, the actual real-use scenario): previously this reproduced a
  session-mirroring bug (see below) where `nsp2_in` streamed an exact
  duplicate of `nsp1_in`'s config. Root-caused upstream in `pycbsdk`
  (`getInstanceNumber()`/shared-memory segment naming didn't account for
  which physical device a session belonged to) and fixed with the new
  `shmem_instance` parameter. With `shmem_instance` set distinctly per
  node, re-ran the full graph end-to-end: `nsp1_in_channel_map` and
  `nsp2_in_channel_map` each independently reported their own correct,
  distinct configs (no mirroring), and both NSPs' `PS4_audio` channels
  captured real signal -- confirmed by concatenating each into a `.wav`
  and cross-correlating them: **0.9998 correlation at a 1-sample lag**,
  i.e. both NSPs captured the exact same real external audio, not silence
  or noise. `notebooks/dual_nsp_audio.ipynb` reproduces this end-to-end
  (start graph -> save -> discover streams from `channel_map` -> export
  every "audio"-labeled channel to `.wav`).
