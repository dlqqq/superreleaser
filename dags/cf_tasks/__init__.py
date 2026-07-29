"""Task groups for the release DAGs, one module per group.

Two top-level composable groups the DAGs assemble from:

- `pypi.pypi_release`         — GitHub Step 1/Step 2 → human gate → PyPI wait
- `conda_forge.conda_forge_release` — the whole conda-forge release

`conda_forge_release` in turn nests the phase groups (`prepare`,
`update_recipe`, `open_pr`, `publish`, `cleanup`) — task groups are recursive.
So `cf_release` is just `conda_forge_release()`, and `e2e_release` is
`pypi_release() >> conda_forge_release()`.

`_common` holds shared bootstrap + the bash ENV + the registry Jinja macro.
Bash I/O lives in `scripts/*.sh` (shown in each task's rendered template);
application logic (recipe edits, dep diff, gate messages) is Python.
"""
