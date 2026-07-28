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
  approval       human approves/rejects in the UI (reject stops the run here)
  wait_for_ci    then poll the PR's checks to green (CI ran during review)
  publish        merge as "<pkg> v<version> (#N)" → poll until downloadable
  cleanup        (parallel, always) delete worktree, close PR if open, delete
                 the fork's remote branch

Each task group lives in its own module under cf_tasks/; the single-use tasks
(CI wait, approval, merge, availability wait, cleanup) are defined inline here.

Safety: the human Reject at the approval gate is the guard — nothing is merged
without an explicit approval in the Airflow UI. Requires Airflow 3.1+ for the
HITL ApprovalOperator (built on 3.3.0 here).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pendulum

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cf_tasks import (
    prepare as prepare_mod,
    update_recipe as update_mod,
    open_pr as open_pr_mod,
    publish as publish_mod,
    cleanup as cleanup_mod,
)
from cf_tasks._common import BASE, ENV, SCRIPTS


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

    # Human approval FIRST, then wait for CI — sequential, not parallel. A reject
    # fails the run before the CI poller ever starts (no stray sensor left
    # polling). This costs ~nothing: CI runs on GitHub the moment the PR opens,
    # so it's usually green by the time approval clears; reviewing the diff
    # doesn't depend on CI anyway.
    approval = ApprovalOperator(
        task_id="approval",
        task_display_name="Await human approval",
        subject="conda-forge release approval",
        body=build_gate_body(pr_url, diff),
        fail_on_reject=True,
    )

    # `gh pr checks --watch` polls internally and normalizes both check types
    # (CheckRun + StatusContext), so it doesn't trip over StatusContext entries
    # that have no `.conclusion`. Exit 0 = all passed; non-zero = a check failed
    # (fails the task); --fail-fast bails on the first failure.
    wait_ci = BashOperator(
        task_id="wait_for_ci",
        task_display_name="Await green CI",
        bash_command='gh pr checks "$PR_URL" --watch --fail-fast --interval 30',
        env={**ENV, "PR_URL": pr_url},
        **BASE,
        doc_md="Block until the PR's checks finish; pass/fail on the result.",
    )

    # publish: merge the approved + CI-green PR, then await conda-forge availability.
    pub = publish_mod.publish(pr_url)

    # cleanup: delete the worktree + close the PR if still open. Runs at the very
    # end regardless of outcome (its tasks are trigger_rule="all_done"); wiring it
    # after the worktree creator + the final publish step fires it on success or
    # partial failure alike.
    clean = cleanup_mod.cleanup()

    pr["open"] >> approval >> wait_ci >> pub["merge"]
    # All cleanup steps hang off the same two anchors (worktree creator + final
    # publish step) and run in parallel.
    anchors = [upd["create_worktree"], pub["await_conda_forge"]]
    anchors >> clean["delete_worktree"]
    anchors >> clean["close_pr"]
    anchors >> clean["delete_remote_branch"]


cf_release()
