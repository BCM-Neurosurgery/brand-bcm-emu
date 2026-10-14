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
  work-in-progress dual-hub port of Stanford's T12 graph; `dual_nsp_audio.yaml`
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
