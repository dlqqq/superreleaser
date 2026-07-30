"""The PyPI/NPM release half as one composable task group.

Runs the source repo's Jupyter Releaser workflows with a human gate between them
and waits for the version to land on PyPI:

  prep_release           gh "Step 1: Prep Release" → watch → capture draft URL
  build_release_gate     fetch the draft's changelog for the approval message
  approval               review the draft; reject → stop (and delete the draft)
  publish_release        gh "Step 2: Publish Release" → publishes to PyPI
  await_pypi             poll PyPI until the version is available
  delete_rejected_draft  run_if a draft is still unpublished: delete it (else skip)

The rejected-draft cleanup is gated with @task.run_if (a declarative condition)
rather than an always-run task that branches internally.
"""

from __future__ import annotations

import subprocess
import urllib.error
import urllib.request

from airflow.sdk import task, task_group
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator

from ._common import BASE, SCRIPTS, env_from, sibling_task_id


@task(task_id="build_release_gate", task_display_name="Build approval message")
def build_release_gate(release_url: str, ident: dict) -> str:
    """Markdown for the gate: the draft-release URL + its changelog body, so the
    reviewer sees exactly what will be published without leaving the UI."""
    package, version, repo = ident["PACKAGE"], ident["VERSION"], ident["REPO"]
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


def split_release_url(release_url: str) -> tuple[str, str]:
    """`https://github.com/owner/repo/releases/tag/TAG` → `("owner/repo", "TAG")`."""
    repo_part, _, tag = release_url.partition("/releases/tag/")
    owner_repo = "/".join(repo_part.rstrip("/").split("/")[-2:])
    return owner_repo, tag


def _draft_still_unpublished(context) -> bool:
    """run_if condition: is there still an unpublished draft to clean up?

    Ground truth via `gh`, not Airflow task state (the Task-SDK runtime context
    has no usable get_task_instance): pull the draft URL from prep_release's
    XCom, then check the release's isDraft. If the human approved, Step 2
    published it (isDraft=false) → False → the task SKIPS. If they rejected (or
    prep failed with no URL), the draft is still a draft → True → clean it up.

    The repo comes from the draft URL itself rather than from `identity` — that
    keeps the condition self-contained, which matters because `identity` sits
    OUTSIDE this group (it's the group's argument, so its task id isn't a
    sibling). Deriving prep_release's id from this task's own id, plus pulling
    from this task's map index, is what makes the condition work at any nesting
    depth — including inside the mapped `release_subpackage` group.
    """
    ti = context["ti"]
    map_index = ti.map_index if ti.map_index is not None and ti.map_index >= 0 else -1
    release_url = ti.xcom_pull(
        task_ids=sibling_task_id(ti.task.task_id, "prep_release"),
        map_indexes=map_index,
    )
    if not release_url:
        return False  # prep never produced a draft → nothing to delete
    repo, tag = split_release_url(release_url)
    out = subprocess.run(
        ["gh", "release", "view", tag, "--repo", repo, "--json",
         "isDraft", "--jq", ".isDraft"],
        capture_output=True, text=True,
    ).stdout.strip()
    return out == "true"


# A Python sensor, not a BashSensor: BashSensor runs its bash_command literally
# (no template_searchpath) and has no append_env, so env= would REPLACE the
# environment and drop PATH. The old workaround — Jinja-templating
# `params.package` into the command — cannot work inside a mapped group, where
# the package comes from the mapped item rather than params.
@task.sensor(
    task_id="await_pypi",
    task_display_name="Await PyPI availability",
    poke_interval=30,
    mode="reschedule",
    timeout=60 * 30,
)
def await_pypi(pypi_name: str, version: str) -> bool:
    """Poll PyPI until the released version's metadata is available."""
    url = f"https://pypi.org/pypi/{pypi_name}/{version}/json"
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            available = r.status == 200
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
        available = False
    print(f"GET {url} → {'available' if available else 'not yet available'}")
    return available


@task_group(group_id="pypi_release", group_display_name="Release on PyPI")
def pypi_release(ident):
    """Full PyPI release via the Jupyter Releaser workflows. Returns the group's
    final success task (await_pypi) so a caller can chain the conda-forge group
    after it."""
    env = env_from(ident)

    # Step 1: prep the draft release and capture its URL (XCom).
    prep = BashOperator(
        task_id="prep_release",
        task_display_name="Prep release (Step 1)",
        bash_command="prep_release.sh",
        env=env,
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
        body=build_release_gate(release_url, ident),
        fail_on_reject=True,
    )

    # Step 2: publish (to PyPI). Only after approval.
    publish = BashOperator(
        task_id="publish_release",
        task_display_name="Publish release (Step 2)",
        bash_command="publish_release.sh",
        env=env_from(ident, RELEASE_URL=release_url),
        **BASE,
        doc_md="Run 'Step 2: Publish Release' with the draft URL, watch to green.",
    )

    awaited = await_pypi(ident["PYPI_NAME"], ident["VERSION"])

    # Conditional cleanup: only runs if an unpublished draft still exists (i.e.
    # rejected/abandoned). @task.run_if is the native declarative gate — the task
    # SKIPS on approval, no hardcoded branch inside it.
    @task.run_if(_draft_still_unpublished)
    @task.bash(
        task_id="delete_rejected_draft",
        task_display_name="Delete rejected draft release",
        env=env_from(ident, RELEASE_URL=release_url),
        append_env=True,
        trigger_rule="all_done",  # reachable even though `approval` failed
    )
    def delete_rejected_draft() -> str:
        # @task.bash runs the returned string (no .sh searchpath lookup like
        # BashOperator), so invoke the script by absolute path.
        return f"bash {SCRIPTS}/delete_draft.sh"

    prep >> approval >> publish >> awaited
    # The rejected-draft cleanup hangs off approval (it only fires on reject).
    approval >> delete_rejected_draft()
    return awaited
