"""conda-forge release DAG for one Jupyter AI subpackage.

A human-gated pipeline that releases an already-published PyPI version to
conda-forge. Trigger with a `dag_run.conf` giving the package and the explicit
target version:

    { "package": "jupyter-ai-acp-client", "version": "0.2.1" }

Flow:

  prepare        clone + fork the feedstock, compute the run-requirement diff,
           │     verify each dep exists on conda-forge
  update_recipe  worktree in /tmp → write recipe → rerender → single commit
           │     (all local — branch fully built before it's pushed)
  open_pr        push the finished branch → open PR titled "<pkg> v<version>"
           ├─ wait_for_ci ─┐   (green CI AND human approval run in parallel;
           └─ approval ────┤    CI runs once, on the final head)
  publish        merge as "<pkg> v<version> (#N)" → poll until downloadable
  cleanup        delete the /tmp worktree + close the PR if still open (always)

Each task group lives in its own module under cf_tasks/; the single-use tasks
(CI wait, approval, merge, availability wait, cleanup) are defined inline here.

Safety: the human Reject at the approval gate is the guard — nothing is merged
without an explicit approval in the Airflow UI. Requires Airflow 3.1+ for the
HITL ApprovalOperator (built on 3.3.0 here).
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pendulum

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.providers.standard.sensors.python import PythonSensor

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cf_tasks import (
    prepare as prepare_mod,
    update_recipe as update_mod,
    open_pr as open_pr_mod,
    publish as publish_mod,
    cleanup as cleanup_mod,
)
from cf_tasks._common import SCRIPTS
from superreleaser import gitops

log = logging.getLogger("superreleaser.dag")


def _ci_green(pr_url: str) -> bool:
    """Sensor poke: True when the PR's checks are all green; raise (fail fast) if
    any check failed; False (keep waiting) while pending."""
    state = gitops.pr_checks_state(pr_url)
    log.info("CI state for %s: %s", pr_url, state)
    if state == "FAILURE":
        raise AirflowFailException(f"feedstock CI failed: {pr_url}")
    return state == "SUCCESS"


@task(task_id="build_gate_body", task_display_name="Build approval message")
def build_gate_body(pr_url: str, diff: dict, **context) -> str:
    """The Markdown shown at the approval gate — PR link + dependency changes."""
    package = context["params"]["package"]
    version = context["params"]["version"]
    lines = [f"### Release `{package}` v{version} to conda-forge", "",
             f"**PR:** {pr_url}", ""]
    if diff:
        lines.append("**Dependency range changes:**")
        for name, change in diff.items():
            lines.append(f"- `{name}`: `{change.get('old') or '—'}` → `{change['new']}`")
    else:
        lines.append("No dependency range changes (version + sha256 bump only).")
    lines += ["", "Approve to **merge** the PR (squash) and await conda-forge "
              "availability, or Reject to fail the run and merge nothing."]
    return "\n".join(lines)


@dag(
    dag_id="cf_release",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["superreleaser", "conda-forge"],
    params={"package": "jupyter-ai-acp-client", "version": ""},
    template_searchpath=[SCRIPTS],
)
def cf_release():
    # prepare: clone/fork + diff + verify dep names on conda-forge.
    diff, verified = prepare_mod.prepare()

    # update_recipe: build the release branch locally (worktree → write →
    # rerender → single commit). Gated behind prepare's dep verification.
    upd = update_mod.update_recipe(diff)
    verified >> upd["create_worktree"]

    # open_pr: push the finished branch + open the PR. Gate its entry behind the
    # commit, so CI runs once on the final head (no rerender-restarts-CI race).
    pr = open_pr_mod.open_pr(upd["worktree_path"])
    upd["commit"] >> pr["push_branch"]
    pr_url = pr["pr_url"]

    # CI wait and human approval run in PARALLEL; both must pass before merge.
    wait_ci = PythonSensor(
        task_id="wait_for_ci",
        task_display_name="Await green CI",
        python_callable=_ci_green,
        op_args=[pr_url],
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60 * 3,
        doc_md="Poll the PR's checks until green; fail the run if CI fails.",
    )

    approval = ApprovalOperator(
        task_id="approval",
        task_display_name="Await human approval",
        subject="conda-forge release approval",
        body=build_gate_body(pr_url, diff),
        fail_on_reject=True,
    )

    # publish: merge the approved PR + await conda-forge availability. Runs only
    # after both gates pass.
    pub = publish_mod.publish(pr_url)

    # cleanup: delete the worktree + close the PR if still open. Runs at the very
    # end regardless of outcome (its tasks are trigger_rule="all_done"); wiring it
    # after the worktree creator + the final publish step fires it on success or
    # partial failure alike.
    clean = cleanup_mod.cleanup()

    [wait_ci, approval] >> pub["merge"]
    [upd["create_worktree"], pub["await_conda_forge"]] >> clean["delete_worktree"]
    [upd["create_worktree"], pub["await_conda_forge"]] >> clean["close_pr"]


cf_release()
