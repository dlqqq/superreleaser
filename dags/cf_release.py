"""conda-forge release DAG for one Jupyter AI subpackage.

A linear, human-gated pipeline that automates the conda-forge feedstock release
headache. Trigger with a `dag_run.conf` of `{"package": "jupyter-ai-acp-client"}`
(optionally `{"version": "0.2.1", "dry_run": true}`). The task bodies live in
`dags/cf_tasks/`; this file is just the DAG shape.

  checkout → pick_version → update_recipe → verify_cf → open_pr
    → wait_for_ci → build_gate_body → approval → merge_pr
    → verify_merge → await_conda_forge

Each `@task`'s docstring is its description in the UI. Requires Airflow 3.1+ for
the HITL ApprovalOperator (built on 3.3.0 here).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pendulum

from airflow.sdk import dag
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.providers.standard.sensors.python import PythonSensor

# Make the sibling cf_tasks/ package importable when Airflow loads this DAG.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from cf_tasks import gate, prepare, publish

# Shared sensor settings: reschedule mode frees the worker slot between polls.
_SENSOR = dict(mode="reschedule", poke_interval=60, timeout=60 * 60 * 3)


@dag(
    dag_id="cf_release",
    schedule=None,  # triggered manually with conf
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["superreleaser", "conda-forge"],
    params={"package": "jupyter-ai-acp-client", "version": "", "dry_run": False},
)
def cf_release():
    # TaskFlow XCom chaining: each task consumes the previous task's return dict.
    checked = prepare.checkout()
    versioned = prepare.pick_version(checked)
    updated = prepare.update_recipe(versioned)
    verified = prepare.verify_cf(updated)
    opened = publish.open_pr(verified)

    wait_for_ci = PythonSensor(
        task_id="wait_for_ci",
        task_display_name="Wait for feedstock CI",
        doc_md="Wait for the rerender commit to land, then require green checks "
        "on that rerendered head (not the brief pre-rerender green). Skipped in "
        "dry-run.",
        python_callable=publish.checks_pass,
        op_args=[opened],
        **_SENSOR,
    )

    gate_body = gate.build_gate_body(opened)
    approval = ApprovalOperator(
        task_id="approval",
        task_display_name="Human approval",
        doc_md="Human-in-the-loop gate. Review the PR in the UI and **Approve** "
        "or **Reject**. Approve → merge + verify the post-merge build. Reject "
        "fails the run and merges nothing.",
        subject="conda-forge release approval",
        body=gate_body,
        fail_on_reject=True,
    )

    merged = publish.merge_pr(opened)

    verify_merge = PythonSensor(
        task_id="verify_merge",
        task_display_name="Verify post-merge build",
        doc_md="After merge, poll CI on the default branch's new head and fail "
        "the run if that build failed. A green build here means conda-forge "
        "uploaded the package. Skipped in dry-run.",
        python_callable=publish.post_merge_ci_ok,
        op_args=[merged],
        **_SENSOR,
    )

    await_conda_forge = PythonSensor(
        task_id="await_conda_forge",
        task_display_name="Await conda-forge availability",
        doc_md="Poll anaconda.org until the merged version appears on the "
        "conda-forge channel. Reached only after a green post-merge build, so "
        "this is bounded CDN propagation, not an open question of whether it "
        "will ship. Skipped in dry-run.",
        python_callable=publish.available_on_conda_forge,
        op_args=[merged],
        **dict(_SENSOR, poke_interval=120, timeout=60 * 60 * 2),
    )

    # Ordering beyond the XCom data-deps: gate the CI wait after the PR, run the
    # approval only once CI is green, and merge → verify → await after approval.
    (opened >> wait_for_ci >> gate_body >> approval >> merged
     >> verify_merge >> await_conda_forge)


cf_release()
