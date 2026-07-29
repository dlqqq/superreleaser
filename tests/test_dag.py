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


def test_expected_tasks_present(cf_dag):
    ids = {t.task_id for t in cf_dag.tasks}
    for expected in [
        "prepare.clone_feedstock", "prepare.ensure_fork", "prepare.get_new_reqs",
        "prepare.compute_req_diff", "prepare.verify_conda",
        "update_recipe.create_worktree", "update_recipe.write_recipe",
        "update_recipe.rerender", "update_recipe.commit",
        "open_pr.push_branch", "open_pr.open",
        "wait_for_ci", "approval",
        "publish.merge", "publish.await_conda_forge",
        "cleanup.delete_worktree", "cleanup.close_pr", "cleanup.delete_remote_branch",
    ]:
        assert expected in ids, f"missing task {expected}"


def test_merge_waits_for_ci(cf_dag):
    # Sequential: approval → wait_for_ci → merge. merge's direct upstream is CI;
    # approval precedes CI (a reject stops the run before CI polls).
    assert "wait_for_ci" in cf_dag.get_task("publish.merge").upstream_task_ids
    assert "approval" in cf_dag.get_task("wait_for_ci").upstream_task_ids


def test_cleanup_runs_regardless(cf_dag):
    for t in ("cleanup.delete_worktree", "cleanup.close_pr", "cleanup.delete_remote_branch"):
        assert cf_dag.get_task(t).trigger_rule == "all_done"


def test_all_bash_scripts_exist(cf_dag, e2e_dag):
    from cf_tasks._common import SCRIPTS
    scripts = Path(SCRIPTS)
    for dag in (cf_dag, e2e_dag):
        for t in dag.tasks:
            cmd = getattr(t, "bash_command", "")
            if cmd.endswith(".sh"):
                assert (scripts / cmd).is_file(), f"{t.task_id} → missing {cmd}"


def test_e2e_expected_tasks(e2e_dag):
    ids = {t.task_id for t in e2e_dag.tasks}
    for expected in [
        "prep_release", "build_release_gate", "approval", "publish_release",
        "await_pypi", "trigger_cf_release", "cleanup_draft",
    ]:
        assert expected in ids, f"missing task {expected}"


def test_e2e_publish_gated_by_approval(e2e_dag):
    # Nothing publishes to PyPI before the human approves.
    assert "approval" in e2e_dag.get_task("publish_release").upstream_task_ids


def test_e2e_hands_off_to_cf_release(e2e_dag):
    trig = e2e_dag.get_task("trigger_cf_release")
    assert trig.trigger_dag_id == "cf_release"
    assert "await_pypi" in trig.upstream_task_ids


def test_e2e_cleanup_runs_regardless(e2e_dag):
    assert e2e_dag.get_task("cleanup_draft").trigger_rule == "all_done"


def test_package_param_is_registry_enum(cf_dag, e2e_dag):
    from superreleaser.registry import PACKAGE_NAMES
    for dag in (cf_dag, e2e_dag):
        enum = dag.params.get_param("package").schema.get("enum")
        assert enum == PACKAGE_NAMES
