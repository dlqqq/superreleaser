"""The PyPI/NPM release half as one composable task group.

Runs the source repo's Jupyter Releaser workflows with a human gate between them
and waits for the version to land on PyPI:

  prep_release           gh "Step 1: Prep Release" → watch → capture draft URL
  build_release_gate     fetch the draft's changelog for the approval message
  approval               review the draft; reject → stop (and delete the draft)
  publish_release        gh "Step 2: Publish Release" → publishes to PyPI
  await_pypi             poll PyPI until the version is available
  delete_rejected_draft  run_if the approval was rejected: delete the draft

The rejected-draft cleanup is gated with @task.run_if (a declarative condition)
rather than an always-run task that branches internally.
"""

from __future__ import annotations

import subprocess

from airflow.sdk import task, task_group
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.providers.standard.sensors.bash import BashSensor

from ._common import BASE, ENV, SCRIPTS
from superreleaser import registry


@task(task_id="build_release_gate", task_display_name="Build approval message")
def build_release_gate(release_url: str, **context) -> str:
    """Markdown for the gate: the draft-release URL + its changelog body, so the
    reviewer sees exactly what will be published without leaving the UI."""
    package = context["params"]["package"]
    version = context["params"]["version"]
    repo = registry.get(package).repo
    tag = release_url.rsplit("/releases/tag/", 1)[-1]

    body = subprocess.run(
        ["gh", "release", "view", tag, "--repo", repo, "--json", "body",
         "--jq", ".body"],
        capture_output=True, text=True,
    ).stdout.strip() or "_(draft release body unavailable)_"

    return "\n".join([
        f"### Publish `{package}` v{version}?",
        "",
        f"**Draft release:** {release_url}",
        "",
        "Review the generated changelog below, then Approve to run "
        "**Step 2: Publish Release** (publishes to PyPI) or Reject to stop and "
        "delete the draft.",
        "",
        "---",
        body,
    ])


def _approval_rejected(context) -> bool:
    """run_if condition: did the human reject the release at the approval gate?
    True when the `approval` task ended in a failed state (fail_on_reject)."""
    ti = context["dag_run"].get_task_instance("pypi_release.approval")
    return ti is not None and str(ti.state) == "failed"


@task_group(group_id="pypi_release", group_display_name="Release on PyPI")
def pypi_release():
    """Full PyPI release via the Jupyter Releaser workflows. Returns the group's
    final success task (await_pypi) so a caller can chain the conda-forge group
    after it."""
    # Step 1: prep the draft release and capture its URL (XCom).
    prep = BashOperator(
        task_id="prep_release",
        task_display_name="Prep release (Step 1)",
        bash_command="prep_release.sh",
        env=ENV,
        **BASE,
        output_processor=lambda o: o.strip().splitlines()[-1],  # draft URL
        doc_md="Run 'Step 1: Prep Release', watch it, and capture the draft "
        "release URL.",
    )
    release_url = prep.output

    approval = ApprovalOperator(
        task_id="approval",
        task_display_name="Await human approval",
        subject="publish release approval",
        body=build_release_gate(release_url),
        fail_on_reject=True,
    )

    # Step 2: publish (to PyPI). Only after approval.
    publish = BashOperator(
        task_id="publish_release",
        task_display_name="Publish release (Step 2)",
        bash_command="publish_release.sh",
        env={**ENV, "RELEASE_URL": release_url},
        **BASE,
        doc_md="Run 'Step 2: Publish Release' with the draft URL, watch to green.",
    )

    await_pypi = BashSensor(
        task_id="await_pypi",
        task_display_name="Await PyPI availability",
        bash_command="curl -fsS -o /dev/null "
        "\"https://pypi.org/pypi/{{ pkg(params.package).pypi_name }}/"
        "{{ params.version }}/json\"",
        poke_interval=30,
        mode="reschedule",
        timeout=60 * 30,
        doc_md="Poll PyPI until the released version's metadata is available.",
    )

    # Conditional cleanup: only runs if the human rejected the draft. @task.run_if
    # is the native declarative gate — no hardcoded branch inside the task.
    @task.run_if(_approval_rejected)
    @task.bash(
        task_id="delete_rejected_draft",
        task_display_name="Delete rejected draft release",
        env={**ENV, "RELEASE_URL": release_url},
        append_env=True,
        trigger_rule="all_done",  # reachable even though `approval` failed
    )
    def delete_rejected_draft() -> str:
        # @task.bash runs the returned string (no .sh searchpath lookup like
        # BashOperator), so invoke the script by absolute path.
        return f"bash {SCRIPTS}/delete_draft.sh"

    prep >> approval >> publish >> await_pypi
    # The rejected-draft cleanup hangs off approval (it only fires on reject).
    approval >> delete_rejected_draft()
    return await_pypi
