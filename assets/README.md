# assets/

Media files (`.mp3`, `.mp4`, `.mov`, `.wav`, ... -- see `../.gitignore`) are
gitignored to keep large binaries out of the repo history, so a fresh clone
has none of them. Copy them in before running any graph that uses
`mediaPlayerTask`.

## How mediaPlayerTask finds its file

Every graph that uses `mediaPlayerTask` (`mediaPlayerTask.yaml`,
`t12_dual_hub.yaml`, `test_mediaPlayerTask_only.yaml`) sets two
parameters:

```yaml
media_folder_path: ../brand-modules/brand-bcm-emu/assets/media_player
media_number: '01'
```

- **`media_folder_path`:** the folder holding the files, here
  `assets/media_player/`. The path is relative to `BRAND/BRAND`, the
  directory the supervisor is launched from.
- **`media_number`:** selects the file whose name starts with that number
  followed by an underscore. `'01'` matches `01_<anything>.<ext>`.
- **Quote it.** Write `'01'`, not `01`, so YAML keeps the leading zero.

So files must be named `NN_<description>.<ext>`, for example
`01_musictask_emuFeb17_E-G_r1_standard.mp3`. The node exits with an
error at startup if no file matches the prefix, or if more than one does.
A file without the `NN_` prefix is never found.

## What the graphs currently select

| Graph | `media_number` |
|---|---|
| `mediaPlayerTask.yaml` | `'10'` |
| `t12_dual_hub.yaml` | `'01'` |
| `test_mediaPlayerTask_only.yaml` | `'01'` |

Get the files from whoever owns the task stimuli (on the BCM rig they
live in `assets/media_player/` on the BRAND machine).
