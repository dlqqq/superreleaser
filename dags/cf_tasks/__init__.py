"""Task definitions for the `cf_release` DAG, split by phase for readability.

The DAG file (`dags/cf_release.py`) imports these and wires them together; each
`@task` function's docstring becomes its `doc_md` in the Airflow UI automatically.

- `prepare`  — checkout, pick_version, update_recipe, verify_cf
- `publish`  — open_pr, merge_pr, and the sensor poke-callables (CI waits,
               conda-forge availability)
- `gate`     — build the approval-gate Markdown body
"""
