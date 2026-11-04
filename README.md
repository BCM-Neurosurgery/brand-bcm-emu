# brand-bcm-emu

BRAND module for BCM's (Baylor College of Medicine) EMU (Epilepsy
Monitoring Unit) rig -- site-specific graphs, nodes, derivatives, and setup
tooling for a single-NSP (and dual-NSP) acquisition chain running entirely
on one Linux machine (no separate Windows "Central PC1" bridge, no
`booter` -- see node/graph comments for details on each).

This module is paired with the shared, multi-site `brand-nsp` module
(`../brand-nsp`), which holds generic, reusable processing nodes
(`re_reference`, `bpExtraction`, `thresholdExtraction`, etc.) used across
several different labs' rigs. This repo is where BCM's own graphs,
site-specific nodes, and one-off setup scripts live instead.

## Layout

- **`graphs/`** -- supergraph YAMLs. `mediaPlayerTask.yaml` is the main
  media-playback-plus-neural-acquisition graph; `t12_dual_hub.yaml` is a
  work-in-progress dual-hub port for Stanford's T12 rig (see
  [Running t12_dual_hub.yaml at Stanford](#running-t12_dual_hubyaml-at-stanford)
  before using it); `dual_nsp_audio.yaml`
  and `test_mediaPlayerTask_only.yaml` are narrower test graphs for the
  dual-NSP acquisition path and the task-controller node in isolation,
  respectively.
- **`nodes/`** -- site-specific node implementations:
  - `cerebusAdapterBCM` -- `pycbsdk`-based NSP acquisition node (see its
    own `README.md` for the auto-discovery design and a documented
    concurrent-session shared-memory bug/fix).
  - `mediaPlayerTask` -- task controller: plays a media file via `mpv`,
    publishes `task_state`/`trial_info`, auto-stops the graph at the end
    of playback.
  - `screen_recorder` -- records the display for the duration of a graph.
  - `nsp_neural_sim` -- shim for testing the acquisition pipeline with
    simulated neural data in place of a real NSP.
- **`derivatives/rdb_to_mat_bcm/`** -- converts a saved RDB from
  `mediaPlayerTask.yaml` into one `.mat` file per Redis stream.
- **`scripts/`** -- standalone, non-supervisor-managed setup utilities.
  See below.
- **`tests/`** -- tests for the above, runnable with `pytest` (no Redis
  required; each test file documents its own requirements).
- **`notebooks/`** -- diagnostic scripts for inspecting NSP/Central
  configuration outside of a running graph (`parse_ccf.py` reads a saved
  `.ccf` file, `poll_nsp_config.py` queries a live NSP via `pycbsdk`,
  `sniff_nsp_packets.py` is a zero-dependency raw-UDP packet sniffer).
- **`assets/`** -- media files for `mediaPlayerTask` and other local test
  data. Not committed to git (see `.gitignore`) -- large binary media
  lives on-disk only.

## Running t12_dual_hub.yaml at Stanford

**This graph does not run as-is.** It ports our media-playback task to
your two-hub rig (two 256-channel neural hubs plus one analog adapter,
merged at the binned-feature layer), but it has never been run on your
hardware, and some values are ours or missing. Below is what we already
know is wrong or unconfirmed, roughly in the order you'd hit it. Several
of these also have comments at the matching spot in the YAML.

1. **Sibling modules.** Module paths resolve relative to `BRAND/BRAND`,
   so these must be checked out under `brand-modules/`:
   - `brand-bcm-emu` (this repo): `mediaPlayerTask`, `screen_recorder`.
   - `brand-nsp`: `re_reference`, `thresholdExtraction`, `bpExtraction`.
     We run commit `c4d4ab4`.
   - `brand-rice`: `bin_multiple`. We run commit `0ede002`. **Nobody has
     confirmed `brand-rice` is on your cart.**
   - `brand-stanford` (yours): the three `cerebusAdapter` nodes and
     `centrals_interface`, transcribed from your own graph config. We
     don't have it, so none of those four have been run with this graph.
2. **Rebuild the binaries.** The `.bin`
   files committed for `mediaPlayerTask` and `screen_recorder` are linked
   against our machine's conda path, so they won't run on yours. Rebuild
   from `BRAND/BRAND` with `rt` active:
   `make ../brand-modules/brand-bcm-emu/nodes/mediaPlayerTask ../brand-modules/brand-bcm-emu/nodes/screen_recorder`.
3. **`broadcast_ip` and `broadcast_device` are missing.** They're
   commented-out `TODO`s on all three `cerebusAdapter` nodes. Your graph
   config only gave us `broadcast_port` and the stream shapes, and we
   didn't guess the NIC side.
4. **CPU affinities.** Only the `stanford_cpu_affinity` block (the three
   adapters and `centrals_interface`) holds your real values. Everything
   in `cpu_affinity_anchors` is ours and needs reassigning. We fitted it
   to the brand-pc layout you described (0-23, performance cores 0-15 as
   pairs, efficiency cores 16-23 single-thread), but it's unvalidated. The
   comment above `cpu_affinity_anchors` has the arithmetic: 13 nodes plus
   `centrals_interface` for 12 slots in cores 8-23, so two slots are
   doubled. We have not measured whether these nodes keep up at 1000 Hz on
   a single efficiency core. `screen_recorder`/`mediaPlayerTask`
   (machine `puget`) also still carry our values.
5. **`participant_file`** (`../Data/t12/t12.yaml`) is a placeholder path.
6. **Run `scripts/make_reref_params.py` before every launch.** If you
   don't, `re_reference` silently falls back to a global mean across all
   channels instead of per-array CAR. Its only sign is the log line
   `Coefficients entry not found in Redis stream: rereference_parameters`.
   Run it after the supervisor has started its Redis (which starts empty)
   and before `startGraph`, with the dual-hub command in the section
   below. `--channels-per-array 64` is our assumption about t12's arrays,
   and without t12's electrode mapping JSON the unshuffling is identity.
7. **Media and task-machine setup** (`puget`):
   - The media files are gitignored and not in the repo. See
     `assets/README.md`. This graph plays the file starting with `01_` in
     `assets/media_player/`. Ask us for it, or use your own.
   - `mediaPlayerTask` needs `mpv`.
   - `screen_recorder` needs `ffmpeg` and
     reads the display from `~/.DISPLAY` (`echo $DISPLAY > ~/.DISPLAY`).
8. **`centrals_interface`.**
   `recording_save_path` is the value from your own graph config.
   Confirm the right path for this block.
9. **Expected warnings.** `ch_mask_stream` is set to `z_mask_stream`,
   which nothing in this graph writes, so every feature node warns that it
   has no entries and uses all channels. That's expected. If you do write
   a mask, note that both hubs' nodes read the same stream using hub-local
   channel indices (0-255), so one mask applies to both hubs.
10. **Field formats from your adapters.** Our feature nodes run with
    `use_tracking_id: True` and `timestamp_data_type: uint64`:
    - **`tracking_id`:** `bpExtraction` and `thresholdExtraction` read it
      from every entry as a uint64. If it's missing or not 8 bytes, they
      crash with a traceback on the first data entry.
      `thresholdExtraction` gets there only after its threshold
      calibration (2000 entries).
    - **`timestamps`:** these are read as uint64 by `re_reference` and
      `mediaPlayerTask`. If yours are uint32, this fails **silently**: a
      30-sample uint32 buffer decodes into 15 wrong uint64 values with no
      error. `re_reference` may print `Timestamps ... are not in order`,
      and the NSP timestamps in `task_state` will be garbage.
    - **Sample layout:** `re_reference` also reshapes `samples` as
      (channels, samples). Check that your adapter writes it that way.
11. **`rdb_to_mat_bcm` is not ported.** The `.mat` export is still set up
    for our single-NSP graph:
    - `neural_n_channels: 256` is one hub's count.
    - `analog_stream: nsp_analog` points at a stream this graph doesn't
      have (`continuousAnalog`).
    - Its stream list expects our single-hub stream names.
    - It drops the hub-2 timestamp fields (`nsp_neural_timestamp_hub_2`,
      `trial_*_nsp_neural_time_hub_2`).

    The raw data is all in the saved RDB, so use your own export for now.

## Running a graph

See each graph YAML's own header comment for specifics, but the general
flow for a single-machine setup (no `booter` needed -- see
`supervisor`'s own `-m`/`machine:` matching behavior):

```bash
conda activate rt
cd /home/eric/BRAND/BRAND
make ../brand-modules/brand-bcm-emu/nodes/<node1> ../brand-modules/brand-bcm-emu/nodes/<node2> ...  # build what the graph needs
supervisor -g ../brand-modules/brand-bcm-emu/graphs/<graph>.yaml -i 127.0.0.1 -p 6379
```

Then, in another terminal, send a `startGraph` command on the
`supervisor_ipstream` Redis stream to actually launch the nodes. Stopping
the graph does **not** save anything by itself -- send a `saverdb` command
first, then run the matching derivative by hand (with `sudo -E`, since
`supervisor`/`booter` run as root and own the `Data/` directory tree).

`cerebusAdapterBCM` depends on this rig's `pycbsdk` install (`nsp_env`
conda env, Python >=3.9) and must be built from that env specifically --
see its own `Makefile`/`README.md`.

## `scripts/make_reref_params.py`

Builds and writes the `rereference_parameters` Redis stream entry that
`brand-nsp`'s `re_reference` node
(`../brand-nsp/nodes/re_reference/re_reference.py`) reads at startup.
Nothing writes this stream automatically -- if you don't run this first,
`re_reference` silently falls back to a plain global mean subtraction
across *all* channels, ignoring array boundaries, instead of real
per-array common average referencing (CAR).

This is a standalone script, not a graph node or derivative: it is not
listed in any graph YAML, has no supervisor lifecycle, and isn't
CPU-pinned. It connects to Redis, writes one entry, and exits. This is
possible because the coefficients only depend on which channel belongs to
which array -- fixed for a given participant -- and never look at live
neural data. (Contrast Stanford's `parameterUpdater.py`, which genuinely
needs to be a derivative, since it fits LRR weights from a diagnostic
block's data every session. Our plain per-array CAR needs no session
data, so it doesn't.)

It lives in this repo rather than `brand-nsp` because it's BCM-site
configuration (our specific channel/hub layout), not a generic reusable
processing node -- it has no Python import dependency on `brand-nsp`, only
a documented Redis wire-contract with `re_reference.py` (see the script's
own module docstring for exactly what's verified and against which
commit).

**Run this once before launching a graph**, since `re_reference` only
reads the stream in its `__init__` and will fall back if the entry isn't
there yet.

Current 256-channel single-hub setup:

```bash
python scripts/make_reref_params.py \
    --chan-total 256 --channels-per-array 64 --hub-sizes 256 \
    --redis-host <host> --redis-port <port>
```

512-channel dual-hub (t12 Stanford port, two 256-channel hubs):

```bash
python scripts/make_reref_params.py \
    --chan-total 512 --channels-per-array 64 --hub-sizes 256 256 \
    --redis-host <host> --redis-port <port>
```

Add `--electrode-map <path/to/mapping.json>` once a participant's
electrode mapping JSON (`{"electrode_mapping": [...]}`, 1-indexed) is
available; without it, `channel_unshuffling` is the identity matrix. This
is currently the case for t12 -- we don't have that JSON yet, so the
dual-hub command above runs with implicit identity unshuffling.

Use `--dry-run` to build and validate the matrices and print a summary
without touching Redis -- useful for checking `--hub-sizes`/
`--channels-per-array` against a real channel-mapping file before writing
anything live.

**Validation**: before writing anything, the script checks that every
`--hub-sizes` block is "closed" under both matrices -- every row and
column sum within a hub's own diagonal block must equal 1.
`re_reference` only ever reads its own hub's diagonal block out of the
full matrix, so any coefficient that lands outside it (e.g. a permutation
that moves a channel across a hub boundary) is silently discarded at
runtime with no error -- that channel just goes dead. The script refuses
to write to Redis if this check fails.

Tests: `tests/test_make_reref_params.py` (pytest, numpy only, no Redis
required).

**Open question, not yet resolved**: our graphs currently run
`bpExtraction` with `enable_CAR: True`, `CAR_group_sizes: 64` (band power
path), while `thresholdExtraction` has CAR off. Once `re_reference` is
actually producing non-identity, non-fallback CAR output (via this
script), the band power path will be CAR'd twice -- once by
`re_reference` and once by `bpExtraction` itself. No graph YAML has been
changed to address this; it's flagged here for whoever wires up the live
electrode mapping and turns this on for real.
