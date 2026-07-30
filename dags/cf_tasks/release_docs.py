"""The `release_docs` task group: run Jupyter Releaser's "Step 0: Prep release
documentation", then approve and merge the changelog PR it opens.

Step 0 generates the release notes and opens them as a draft PR against the
target branch. Step 1 builds the release from that branch, so the notes must be
merged first — the DAG does the merge, gated on a human reading the generated
changelog (a locked decision in PLAN-simple-jai-release.md).

  step0_docs      — run Step 0, watch it, capture the docs PR URL
  approval        — human reads the generated release notes
  wait_for_ci     — the docs PR's checks must pass
  merge_docs_pr   — mark ready (Step 0 opens a draft) + squash-merge
"""

from __future__ import annotations

import subprocess

from airflow.sdk import task, task_group
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator

from ._common import BASE


@task(task_id="build_docs_gate", task_display_name="Build approval message")
def build_docs_gate(pr_url: str, repo: str, version: str) -> str:
    """The Markdown shown at the docs gate — the PR link plus its diff summary.

    The generated notes themselves are the thing under review, so the gate shows
    the PR body (Step 0 summarizes the changelog there) rather than making the
    reviewer guess from a bare URL.
    """
    body = subprocess.run(
        ["gh", "pr", "view", pr_url, "--repo", repo, "--json", "body", "--jq", ".body"],
        capture_output=True, text=True,
    ).stdout.strip() or "_(PR body unavailable — open the PR to review the notes)_"

    return "\n".join([
        f"### Merge the release notes for `jupyter-ai` {version}?",
        "",
        f"**Docs PR:** {pr_url}",
        "",
        "Review the generated changelog, then Approve to squash-merge it (Step 1 "
        "builds the release from the merged branch) or Reject to fail the run and "
        "merge nothing. Edit the notes on the PR branch first if they need "
        "fixing — this merges whatever the branch holds at approval time.",
        "",
        "---",
        body,
    ])


@task_group(group_id="release_docs", group_display_name="Prep release docs")
def release_docs(repo: str, version: str, target_branch: str = "main",
                 entry_trigger_rule: str = "all_success"):
    """Run Step 0 and merge the resulting changelog PR.

    Returns `{"entry", "exit"}` handles: the group is entered at `step0_docs`, so
    chaining onto the returned merge task alone would leave Step 0 with no
    upstream. `entry_trigger_rule` applies to that entry task only — the upstream
    bump group legitimately skips when there's nothing to bump, and Step 0 must
    still run in that case.
    """
    step0 = BashOperator(
        task_id="step0_docs",
        task_display_name="Prep release docs (Step 0)",
        bash_command="prep_release_docs.sh",
        env={"REPO": repo, "VERSION": version, "TARGET_BRANCH": target_branch},
        trigger_rule=entry_trigger_rule,
        **BASE,
        output_processor=lambda o: o.strip().splitlines()[-1],  # docs PR URL
        doc_md="Run 'Step 0: Prep release documentation', watch it, and capture "
        "the changelog PR it opens.",
    )
    pr_url = step0.output

    approval = ApprovalOperator(
        task_id="approval",
        task_display_name="Await human approval",
        subject=f"release notes for jupyter-ai {version}",
        body=build_docs_gate(pr_url, repo, version),
        fail_on_reject=True,
    )

    wait_ci = BashOperator(
        task_id="wait_for_ci",
        task_display_name="Await green CI",
        bash_command='gh pr checks "$PR_URL" --watch --fail-fast --interval 30',
        env={"PR_URL": pr_url},
        **BASE,
        doc_md="Block until the docs PR's checks finish; pass/fail on the result.",
    )

    merge = BashOperator(
        task_id="merge_docs_pr",
        task_display_name="Merge the docs PR",
        bash_command="merge_source_pr.sh",
        env={"REPO": repo, "PR_URL": pr_url},
        **BASE,
        doc_md="Mark the draft docs PR ready and squash-merge it, so Step 1 "
        "builds a release whose changelog is already in the branch.",
    )

    step0 >> approval >> wait_ci >> merge
    return {"entry": step0, "exit": merge}
