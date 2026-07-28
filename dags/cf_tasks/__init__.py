"""Task groups for the `cf_release` DAG, one module per group.

The DAG (`dags/cf_release.py`) imports these and wires them together; each
`@task` function's docstring becomes its `doc_md` in the Airflow UI.

- `prepare`       — clone + fork the feedstock, compute the run-requirement diff,
                    verify each dep exists on conda-forge
- `update_recipe` — build the release branch locally (worktree → write recipe →
                    rerender → single commit)
- `open_pr`       — push the finished branch and open the PR
- `_common`       — shared bootstrap + constants (SCRIPTS dir, bash env)

Single-use tasks (CI wait, approval, merge, availability wait, cleanup) are
defined inline in the DAG rather than here.
"""
