"""The `cleanup` task group: tidy up after a release run, whatever the outcome.

Wired with trigger_rule="all_done" upstream so it runs on success, failure, or a
rejected gate. Three independent best-effort steps (they run in parallel — no
ordering between them):

  1. delete_worktree      — remove the /tmp release worktree + local branch
  2. close_pr             — close the release PR if still open (a merged PR is
                            not "open", so a successful release is untouched)
  3. delete_remote_branch — delete the fork's remote release branch if it exists
"""

from __future__ import annotations

from airflow.sdk import task_group
from airflow.providers.standard.operators.bash import BashOperator

from ._common import BASE, env_from


@task_group(group_id="cleanup", group_display_name="Clean up")
def cleanup(ident):
    """Remove the worktree and close the PR if still open (both best-effort)."""
    env = env_from(ident)

    delete_worktree = BashOperator(
        task_id="delete_worktree",
        task_display_name="Delete worktree",
        bash_command="cleanup.sh",
        env=env,
        **BASE,
        trigger_rule="all_done",
        doc_md="Remove the /tmp release worktree (best-effort).",
    )

    close_pr = BashOperator(
        task_id="close_pr",
        task_display_name="Close PR if open",
        bash_command="close_pr.sh",
        env=env,
        **BASE,
        trigger_rule="all_done",
        doc_md="Close the release PR if it's still open (no-op once merged).",
    )

    delete_remote_branch = BashOperator(
        task_id="delete_remote_branch",
        task_display_name="Delete fork branch",
        bash_command="delete_remote_branch.sh",
        env=env,
        **BASE,
        trigger_rule="all_done",
        doc_md="Delete the fork's remote release branch if it exists.",
    )

    # No dependencies between them → all three run in parallel.
    return {
        "delete_worktree": delete_worktree,
        "close_pr": close_pr,
        "delete_remote_branch": delete_remote_branch,
    }
