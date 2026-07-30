"""DAG-integrity tests: every DAG imports cleanly, has the expected shape, and
every BashOperator references a real script on the searchpath."""

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
    from airflow.dag_processing.dagbag import DagBag
    db = DagBag(dag_folder=str(_REPO / "dags"))
    assert not db.import_errors, db.import_errors
    return db


@pytest.fixture(scope="module")
def cf_dag(dagbag):
    return dagbag.dags["cf_release"]


@pytest.fixture(scope="module")
def e2e_dag(dagbag):
    return dagbag.dags["e2e_release"]


@pytest.fixture(scope="module")
def jai_dag(dagbag):
    return dagbag.dags["simple_jai_release"]


# In cf_release the conda-forge group is the whole DAG, so its tasks sit under
# the "conda_forge_release." prefix.
_CF = "conda_forge_release"


def test_expected_tasks_present(cf_dag):
    ids = {t.task_id for t in cf_dag.tasks}
    for suffix in [
        "prepare.clone_feedstock", "prepare.ensure_fork", "prepare.get_new_reqs",
        "prepare.compute_req_diff", "prepare.verify_conda",
        "update_recipe.create_worktree", "update_recipe.write_recipe",
        "update_recipe.rerender", "update_recipe.commit",
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


def test_cleanup_runs_regardless(cf_dag):
    for t in ("cleanup.delete_worktree", "cleanup.close_pr", "cleanup.delete_remote_branch"):
        assert cf_dag.get_task(f"{_CF}.{t}").trigger_rule == "all_done"


def test_all_bash_scripts_exist(cf_dag, e2e_dag, jai_dag):
    from cf_tasks._common import SCRIPTS
    scripts = Path(SCRIPTS)
    for dag in (cf_dag, e2e_dag, jai_dag):
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


def test_rejected_draft_cleanup_is_conditional_and_all_done(e2e_dag):
    t = e2e_dag.get_task("pypi_release.delete_rejected_draft")
    assert t.trigger_rule == "all_done"
    # @task.run_if installs its condition as a pre-execute hook; a plain task
    # wouldn't have one. This distinguishes the conditional gate from an
    # always-run task that branches internally.
    assert t._pre_execute_hook is not None


def test_package_param_is_registry_enum(cf_dag, e2e_dag):
    from superreleaser.registry import PACKAGE_NAMES
    for dag in (cf_dag, e2e_dag):
        enum = dag.params.get_param("package").schema.get("enum")
        assert enum == PACKAGE_NAMES


def test_identity_pushes_each_key_separately(cf_dag, e2e_dag, jai_dag):
    # `env_from` builds its env as ident["PACKAGE"] etc. — keyed XCom pulls that
    # resolve only if the task pushed each key as its own XCom. Without
    # multiple_outputs only `return_value` exists and EVERY bash step fails with
    # XComNotFound, so assert it on the instantiated tasks.
    for dag, task_id in ((cf_dag, "identity"), (e2e_dag, "identity"),
                         (jai_dag, "jai_identity"),
                         (jai_dag, "release_subpackage.identity")):
        assert dag.get_task(task_id).multiple_outputs is True, f"{dag.dag_id}.{task_id}"


# --------------------------------------------------------------------------- #
# simple_jai_release: a mapped Phase A, then a sequential jupyter-ai Phase B.
# --------------------------------------------------------------------------- #
_SUB = "release_subpackage"


def test_subpackage_release_is_a_mapped_group(jai_dag):
    from airflow.sdk.definitions.taskgroup import MappedTaskGroup
    groups = jai_dag.task_group.get_task_group_dict()
    assert isinstance(groups[_SUB], MappedTaskGroup)
    # Each expansion runs the FULL e2e release, resolving its own package.
    ids = {t.task_id for t in jai_dag.tasks}
    for suffix in ("identity", "pypi_release.prep_release",
                   "conda_forge_release.publish.await_conda_forge"):
        assert f"{_SUB}.{suffix}" in ids


def test_mapped_source_is_a_plain_return_value(jai_dag):
    # expand_kwargs() rejects a keyed XComArg, so the mapped list must come from
    # its own task rather than plan_release["to_release"].
    assert "to_release" in {t.task_id for t in jai_dag.tasks}
    assert "plan_release" in jai_dag.get_task("to_release").upstream_task_ids


def test_phase_b_survives_an_empty_expansion(jai_dag):
    # When every subpackage is already published the mapped group expands to zero
    # instances and is SKIPPED. An all_success entry would skip too — silently
    # abandoning the jupyter-ai release in the re-run case we explicitly support.
    entry = jai_dag.get_task("bump_ranges.clone_source")
    assert entry.trigger_rule == "none_failed"
    assert f"{_SUB}.conda_forge_release.publish.await_conda_forge" in entry.upstream_task_ids


def test_phase_b_survives_a_skipped_bump(jai_dag):
    # bump_ranges short-circuits when no range changed; Step 0 must still run.
    assert jai_dag.get_task("release_docs.step0_docs").trigger_rule == "none_failed"
    assert jai_dag.get_task("jai_identity").trigger_rule == "none_failed"


def test_merge_tasks_are_not_loosened(jai_dag):
    # The counterpart to the two tests above: a `none_failed` merge would merge a
    # PR that the short-circuit never opened. Only the ENTRY tasks are loosened.
    for t in ("bump_ranges.merge_bump_pr", "release_docs.merge_docs_pr"):
        assert jai_dag.get_task(t).trigger_rule == "all_success", t


def test_phase_b_is_sequential_after_the_wave(jai_dag):
    # bump → docs → jupyter-ai's own e2e release, in that order.
    chain = [
        ("bump_ranges.merge_bump_pr", "release_docs.step0_docs"),
        ("release_docs.merge_docs_pr", "jai_identity"),
        ("jai_identity", "pypi_release.prep_release"),
        ("pypi_release.await_pypi", "conda_forge_release.prepare.clone_feedstock"),
    ]
    for upstream, downstream in chain:
        assert upstream in jai_dag.get_task(downstream).upstream_task_ids, \
            f"{upstream} → {downstream}"


def test_bump_and_docs_merges_wait_for_approval_and_ci(jai_dag):
    for group in ("bump_ranges", "release_docs"):
        merge = "merge_bump_pr" if group == "bump_ranges" else "merge_docs_pr"
        up = jai_dag.get_task(f"{group}.{merge}").upstream_task_ids
        assert f"{group}.wait_for_ci" in up
        assert f"{group}.approval" in \
            jai_dag.get_task(f"{group}.wait_for_ci").upstream_task_ids
