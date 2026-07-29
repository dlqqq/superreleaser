"""The whole conda-forge release as one composable task group.

This wraps the full pipeline (prepare → update recipe → open PR → approve →
await CI → publish → clean up) so it can be dropped into any DAG as a single
node. `cf_release` is just this group; `e2e_release` runs it after the PyPI
release group. Task groups nest, so the inner phase groups (Prepare, Update
feedstock, …) still show up nested underneath.
"""

from __future__ import annotations

from airflow.sdk import task, task_group
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator

from . import prepare as prepare_mod
from . import update_recipe as update_mod
from . import open_pr as open_pr_mod
from . import publish as publish_mod
from . import cleanup as cleanup_mod
from ._common import BASE, ENV


@task(task_id="build_gate_body", task_display_name="Build approval message")
def build_gate_body(pr_url: str, diff: dict, **context) -> str:
    """The Markdown shown at the conda-forge approval gate — PR link + dep changes."""
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


@task_group(group_id="conda_forge_release", group_display_name="Release on Conda Forge")
def conda_forge_release():
    """Full conda-forge release. Returns {"entry", "exit"} handles so a caller
    can gate the group's start (entry) and chain after its end (exit)."""
    # prepare: clone/fork + diff + verify dep names on conda-forge.
    diff, verified, prepare_entry = prepare_mod.prepare()

    # update_recipe: build the release branch locally (worktree → write →
    # rerender → single commit). Gated behind prepare's dep verification.
    upd = update_mod.update_recipe(diff)
    verified >> upd["create_worktree"]

    # open_pr: push the finished branch + open the PR. Gate its entry behind the
    # commit, so CI runs once on the final head (no rerender-restarts-CI race).
    pr = open_pr_mod.open_pr(upd["worktree_path"])
    upd["commit"] >> pr["push_branch"]
    pr_url = pr["pr_url"]

    # Human approval FIRST, then wait for CI — sequential. A reject fails the run
    # before the CI poller starts (no stray sensor). Costs ~nothing: CI runs on
    # GitHub the moment the PR opens, so it's usually green by approval time.
    approval = ApprovalOperator(
        task_id="approval",
        task_display_name="Await human approval",
        subject="conda-forge release approval",
        body=build_gate_body(pr_url, diff),
        fail_on_reject=True,
    )

    # `gh pr checks --watch` polls internally and normalizes both check types
    # (CheckRun + StatusContext); exit 0 = passed, non-zero = failed.
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

    # cleanup (parallel, all_done): delete worktree, close PR if open, delete fork
    # branch. Anchored on the worktree creator + the final publish step so it
    # runs on success or partial failure alike.
    clean = cleanup_mod.cleanup()

    pr["open"] >> approval >> wait_ci >> pub["merge"]
    anchors = [upd["create_worktree"], pub["await_conda_forge"]]
    anchors >> clean["delete_worktree"]
    anchors >> clean["close_pr"]
    anchors >> clean["delete_remote_branch"]

    return {"entry": prepare_entry, "exit": pub["await_conda_forge"]}
