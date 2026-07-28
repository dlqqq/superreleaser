"""Approval-gate phase: build the Markdown shown to the human at the gate."""

from __future__ import annotations

from airflow.sdk import task


@task(task_display_name="Build approval message")
def build_gate_body(ctx: dict) -> str:
    """Assemble the Markdown shown at the approval gate — PR link, run
    requirements, and any unresolved/unsatisfied deps."""
    lines = [
        f"### Review conda-forge PR for `{ctx['package']}` v{ctx['target']}",
        "",
        f"**PR:** {ctx['pr_url']}  {'(DRAFT — unresolved deps)' if ctx['draft'] else ''}",
        "",
        "**Run requirements:**",
        *[f"- `{r}`" for r in ctx["run"]],
    ]
    if ctx["unresolved"]:
        lines += ["", "**⚠️ Unresolved deps (not added — fix before merge):**",
                  *[f"- `{u}`" for u in ctx["unresolved"]]]
    if ctx["unsatisfied"]:
        lines += ["", "**⚠️ Ranges with no matching conda-forge build yet:**",
                  *[f"- `{u}`" for u in ctx["unsatisfied"]]]
    lines += ["", "Approve to **merge** this PR (squash) and verify the "
              "post-merge build, or Reject to fail the run and merge nothing."]
    return "\n".join(lines)
