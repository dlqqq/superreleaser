"""End-to-end release DAG for a Jupyter extension package.

Runs the GitHub release workflows with a human gate in between, waits for PyPI,
then hands off to the conda-forge release DAG. Trigger with a package (dropdown)
and an explicit version:

    { "package": "jupyter-ai-acp-client", "version": "0.2.2" }

Flow:
  prep_release        gh run "Step 1: Prep Release" → watch → capture draft URL
  build_release_gate  fetch the draft release's body for the approval message
  approval            human reviews the draft (URL + changelog); reject → stop
  publish_release     gh run "Step 2: Publish Release" (draft URL) → watch
  await_pypi          BashSensor: poll PyPI until the version's sdist is up
  trigger_cf_release  TriggerDagRunOperator → the cf_release DAG (waits for it)
  cleanup_draft       all_done: if a draft was made but never published, delete it

The conda-forge half is the existing cf_release DAG, invoked via
TriggerDagRunOperator (not duplicated). Requires Airflow 3.1+ for the HITL gate.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pendulum

from airflow.sdk import dag, task, Param
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.bash import BashSensor

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cf_tasks._common import BASE, ENV, MACROS, SCRIPTS
from superreleaser.registry import PACKAGE_NAMES


@task(task_id="build_release_gate", task_display_name="Build approval message")
def build_release_gate(release_url: str, **context) -> str:
    """Markdown for the gate: the draft-release URL + its changelog body, so the
    reviewer sees exactly what will be published without leaving the UI."""
    import subprocess
    from superreleaser import registry

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


@dag(
    dag_id="e2e_release",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["superreleaser", "release"],
    params={
        "package": Param("jupyter-ai-acp-client", type="string", enum=PACKAGE_NAMES),
        "version": Param("", type="string"),
    },
    template_searchpath=[SCRIPTS],
    user_defined_macros=MACROS,
)
def e2e_release():
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

    gate_body = build_release_gate(release_url)
    approval = ApprovalOperator(
        task_id="approval",
        task_display_name="Await human approval",
        subject="publish release approval",
        body=gate_body,
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

    # Wait for the version to actually appear on PyPI (sdist JSON = 200).
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

    # Hand off to the conda-forge release DAG (reuse, don't duplicate).
    trigger_cf = TriggerDagRunOperator(
        task_id="trigger_cf_release",
        task_display_name="Release on conda-forge",
        trigger_dag_id="cf_release",
        conf={"package": "{{ params.package }}", "version": "{{ params.version }}"},
        wait_for_completion=True,
        poke_interval=60,
        deferrable=True,
        failed_states=["failed"],
        doc_md="Trigger the cf_release DAG for the same package/version and wait "
        "for it to finish.",
    )

    # Cleanup: delete the draft if it was created but never published. all_done so
    # it runs on reject/failure too; the script no-ops on a published release.
    cleanup_draft = BashOperator(
        task_id="cleanup_draft",
        task_display_name="Delete abandoned draft",
        bash_command="delete_draft.sh",
        env={**ENV, "RELEASE_URL": release_url},
        **BASE,
        trigger_rule="all_done",
        doc_md="Delete the draft release if it exists and was never published.",
    )

    prep >> gate_body >> approval >> publish >> await_pypi >> trigger_cf
    [prep, trigger_cf] >> cleanup_draft


e2e_release()
