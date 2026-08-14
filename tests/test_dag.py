"""DAG-integrity tests for cf_release: it imports cleanly, has the expected
shape, and every BashOperator references a real script on the searchpath."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "dags"))
os.environ.setdefault("AIRFLOW_HOME", str(_REPO / ".airflow"))
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(_REPO / "dags")
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models.dagbag import DagBag
    db = DagBag(dag_folder=str(_REPO / "dags"))
    assert not db.import_errors, db.import_errors
    return db


@pytest.fixture(scope="module")
def cf_dag(dagbag):
    return dagbag.dags["cf_release"]


@pytest.fixture(scope="module")
def e2e_dag(dagbag):
    return dagbag.dags["e2e_release"]


# In cf_release the conda-forge group is the whole DAG, so its tasks sit under
# the "conda_forge_release." prefix.
_CF = "conda_forge_release"


def test_expected_tasks_present(cf_dag):
    ids = {t.task_id for t in cf_dag.tasks}
    for suffix in [
        "prepare.clone_feedstock", "prepare.ensure_fork", "prepare.get_new_reqs",
        "prepare.compute_req_diff", "prepare.verify_conda",
        "update_recipe.create_worktree", "update_recipe.write_recipe",
        "update_recipe.upgrade_smithy", "update_recipe.rerender", "update_recipe.commit",
        "open_pr.push_branch", "open_pr.open",
        "wait_for_ci", "approval",
        "publish.merge", "publish.await_conda_forge",
        "cleanup.delete_worktree", "cleanup.close_pr", "cleanup.delete_remote_branch",
    ]:
        assert f"{_CF}.{suffix}" in ids, f"missing task {_CF}.{suffix}"


def test_merge_waits_for_ci(cf_dag):
    # Sequential: approval → wait_for_ci → merge.
    assert f"{_CF}.wait_for_ci" in cf_dag.get_task(f"{_CF}.publish.merge").upstream_task_ids
    assert f"{_CF}.approval" in cf_dag.get_task(f"{_CF}.wait_for_ci").upstream_task_ids


def test_rerender_waits_for_smithy_upgrade(cf_dag):
    # conda-smithy is upgraded to the newest allowed version before rerender, so
    # rerender never aborts on its own staleness guard.
    up = cf_dag.get_task(f"{_CF}.update_recipe.rerender").upstream_task_ids
    assert f"{_CF}.update_recipe.upgrade_smithy" in up


def test_cleanup_runs_regardless(cf_dag):
    for t in ("cleanup.delete_worktree", "cleanup.close_pr", "cleanup.delete_remote_branch"):
        assert cf_dag.get_task(f"{_CF}.{t}").trigger_rule == "all_done"


def test_all_bash_scripts_exist(cf_dag, e2e_dag):
    from cf_tasks._common import SCRIPTS
    scripts = Path(SCRIPTS)
    for dag in (cf_dag, e2e_dag):
        for t in dag.tasks:
            cmd = getattr(t, "bash_command", "")
            if isinstance(cmd, str) and cmd.endswith(".sh"):
                assert (scripts / cmd).is_file(), f"{t.task_id} → missing {cmd}"


def test_e2e_is_two_groups_in_sequence(e2e_dag):
    ids = {t.task_id for t in e2e_dag.tasks}
    # Both group phases present.
    assert "pypi_release.prep_release" in ids
    assert "conda_forge_release.prepare.clone_feedstock" in ids
    # conda-forge's entry starts only after PyPI is available.
    entry = e2e_dag.get_task("conda_forge_release.prepare.clone_feedstock")
    assert "pypi_release.await_pypi" in entry.upstream_task_ids


def test_e2e_publish_gated_by_approval(e2e_dag):
    # Nothing publishes to PyPI before the human approves.
    up = e2e_dag.get_task("pypi_release.publish_release").upstream_task_ids
    assert "pypi_release.approval" in up


def test_rejected_draft_cleanup_is_conditional_and_one_failed(e2e_dag):
    t = e2e_dag.get_task("pypi_release.delete_rejected_draft")
    # one_failed: fires only when a parent failed (reject / publish failure),
    # skipped on a clean published run.
    assert t.trigger_rule == "one_failed"
    # @task.run_if installs its condition as a pre-execute hook; a plain task
    # wouldn't have one. This is the belt-and-suspenders isDraft guard.
    assert t._pre_execute_hook is not None


def test_rejected_draft_cleanup_child_of_approval_only(e2e_dag):
    # Reject-only: approval is the sole *gating* parent so the draft is deleted
    # only on a rejected gate. (prep_release is also upstream — implicitly, via
    # the RELEASE_URL XCom it consumes — but that carries no trigger semantics.)
    # It must NOT watch publish_release (a late Step 2 failure can still have
    # published the release — its notes must be left intact) nor await_pypi.
    t = e2e_dag.get_task("pypi_release.delete_rejected_draft")
    assert "pypi_release.approval" in t.upstream_task_ids
    assert "pypi_release.publish_release" not in t.upstream_task_ids
    assert "pypi_release.await_pypi" not in t.upstream_task_ids


def test_prep_release_always_since_last_stable():
    # Step 1 must always pass jupyter-releaser's `since_last_stable` boolean
    # input (defaults to false/unchecked) so the changelog is built from PRs
    # since the last *stable* tag.
    from cf_tasks._common import SCRIPTS
    script = (Path(SCRIPTS) / "prep_release.sh").read_text()
    assert "-f since_last_stable=true" in script


def test_package_param_is_registry_enum(cf_dag, e2e_dag):
    from superreleaser.registry import PACKAGE_NAMES
    for dag in (cf_dag, e2e_dag):
        enum = dag.params.get_param("package").schema.get("enum")
        assert enum == PACKAGE_NAMES
