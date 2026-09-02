# assets/

`.mov`/`.mp3` media files are gitignored here (see `../.gitignore`) to avoid
bloating repo history with large binaries. `graphs/mediaPlayerTask.yaml`
expects these two files to exist locally before running:

- `timeline.mov` -- video played by the `mediaPlayerTask` node
  (`media_file_path` parameter in the graph YAML).
- `long_stroll.mp3` -- referenced audio asset.

Copy them in from wherever the current session's media lives (e.g. from
`brand-bcm-emu-1/assets/` on a machine that still has them, or from
whoever owns the task stimuli) before starting the graph.
