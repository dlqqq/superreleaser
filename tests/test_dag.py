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
def cf_dag():
    from airflow.models.dagbag import DagBag
    db = DagBag(dag_folder=str(_REPO / "dags"))
    assert not db.import_errors, db.import_errors
    return db.dags["cf_release"]


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
        "cleanup.delete_worktree", "cleanup.close_pr",
    ]:
        assert expected in ids, f"missing task {expected}"


def test_merge_waits_for_ci(cf_dag):
    # Sequential: approval → wait_for_ci → merge. merge's direct upstream is CI;
    # approval precedes CI (a reject stops the run before CI polls).
    assert "wait_for_ci" in cf_dag.get_task("publish.merge").upstream_task_ids
    assert "approval" in cf_dag.get_task("wait_for_ci").upstream_task_ids


def test_cleanup_runs_regardless(cf_dag):
    for t in ("cleanup.delete_worktree", "cleanup.close_pr"):
        assert cf_dag.get_task(t).trigger_rule == "all_done"


def test_all_bash_scripts_exist(cf_dag):
    from cf_tasks._common import SCRIPTS
    scripts = Path(SCRIPTS)
    for t in cf_dag.tasks:
        cmd = getattr(t, "bash_command", "")
        if cmd.endswith(".sh"):
            assert (scripts / cmd).is_file(), f"{t.task_id} → missing {cmd}"
