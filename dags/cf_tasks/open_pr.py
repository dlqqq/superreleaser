"""The `open_pr` task group: push the finished release branch and open the PR.

Runs after `update_recipe` has built the branch locally (recipe bump + rerender
in a single commit). Because the branch is complete before it's pushed, CI runs
exactly once — on the final head.

  1. push_branch — push the release branch to the fork
  2. open        — open the PR titled exactly "<pkg> v<version>" (so a squash
                   merge yields "<pkg> v<version> (#N)"); returns the PR URL
"""

from __future__ import annotations

from airflow.sdk import task_group
from airflow.providers.standard.operators.bash import BashOperator

from ._common import BASE, ENV


@task_group(group_id="open_pr", group_display_name="Open feedstock PR")
def open_pr(worktree_path):
    """Push the finished branch to the fork and open the PR. Returns a dict:
      - `pr_url` (XComArg) — the opened PR's URL;
      - `push_branch` (task) — the entry task, to gate behind the commit;
      - `open` (task) — the open-PR task, for downstream ordering.
    """
    wt = {"WORKTREE": worktree_path}

    push_branch = BashOperator(
        task_id="push_branch",
        bash_command="push_branch.sh",
        env={**ENV, **wt},
        **BASE,
        doc_md="Push the finished release branch to the fork.",
    )

    open_pr_task = BashOperator(
        task_id="open",
        bash_command="open_pr.sh",
        env={**ENV, **wt},
        **BASE,
        output_processor=lambda o: o.strip().splitlines()[-1],  # PR URL (last line)
        doc_md="Open the feedstock PR titled exactly `<pkg> v<version>`; returns "
        "the PR URL.",
    )

    push_branch >> open_pr_task
    return {
        "pr_url": open_pr_task.output,
        "push_branch": push_branch,
        "open": open_pr_task,
    }
