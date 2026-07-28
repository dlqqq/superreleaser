"""The `cleanup` task group: tidy up after a release run, whatever the outcome.

Wired with trigger_rule="all_done" upstream so it runs on success, failure, or a
rejected gate. Two independent best-effort steps:

  1. delete_worktree — remove the /tmp release worktree
  2. close_pr        — close the release PR if it's still open (a merged PR is
                       not "open", so a successful release is untouched)
"""

from __future__ import annotations

from airflow.sdk import task_group
from airflow.providers.standard.operators.bash import BashOperator

from ._common import BASE, ENV


@task_group(group_id="cleanup", group_display_name="Clean up")
def cleanup():
    """Remove the worktree and close the PR if still open (both best-effort)."""
    delete_worktree = BashOperator(
        task_id="delete_worktree",
        task_display_name="Delete worktree",
        bash_command="cleanup.sh",
        env=ENV,
        **BASE,
        trigger_rule="all_done",
        doc_md="Remove the /tmp release worktree (best-effort).",
    )

    close_pr = BashOperator(
        task_id="close_pr",
        task_display_name="Close PR if open",
        bash_command="close_pr.sh",
        env=ENV,
        **BASE,
        trigger_rule="all_done",
        doc_md="Close the release PR if it's still open (no-op once merged).",
    )

    return {"delete_worktree": delete_worktree, "close_pr": close_pr}
