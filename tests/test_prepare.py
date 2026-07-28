"""Unit tests for the prepare task group's output_processor helpers.

These test the pure parsing/diff logic with no Airflow and no network (the
conda-forge name resolution is monkeypatched). The bash I/O itself is exercised
end-to-end by `just test-prep`.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))

from cf_tasks import prepare  # noqa: E402


RECIPE = """\
context:
  version: "0.2.0"
package:
  name: jupyter-ai-acp-client
requirements:
  run:
    - python >=${{ python_min }}
    - jupyter_server >=2.4.0,<3
    - pydantic >=2,<3
"""


@pytest.fixture
def recipe_file(tmp_path):
    """Write RECIPE to a temp feedstock layout and point config there, so
    compute_req_diff (which reads config.recipe_path(package)) finds it."""
    from superreleaser import config
    fs = tmp_path / "jupyter-ai-acp-client-feedstock" / "recipe"
    fs.mkdir(parents=True)
    (fs / "recipe.yaml").write_text(RECIPE)
    orig = config.FEEDSTOCKS_ROOT
    config.FEEDSTOCKS_ROOT = tmp_path
    yield str(fs / "recipe.yaml")
    config.FEEDSTOCKS_ROOT = orig


# The Python task's underlying function (unwrapped) — call it directly with a
# fake context in place of Airflow's params injection.
_compute = prepare.compute_req_diff.function
_PKG = {"params": {"package": "jupyter-ai-acp-client"}}


def test_proc_pypi_requirements_drops_extras_and_sorts():
    pypi = json.dumps({"info": {"requires_dist": [
        "jupyter-server<3,>=2.4.0",
        "pydantic<3,>=2",
        "pytest; extra == \"test\"",       # extra → dropped
    ]}})
    reqs = prepare._proc_pypi_requirements(pypi)
    assert reqs == [
        {"name": "jupyter-server", "spec": ">=2.4.0,<3"},   # floor-first
        {"name": "pydantic", "spec": ">=2,<3"},
    ]


def test_compute_diff_maps_conda_names_and_ranges(recipe_file):
    # jupyter-server (PyPI) must map to the recipe's existing jupyter_server.
    reqs = [{"name": "jupyter-server", "spec": ">=2.5.0,<3"},
            {"name": "pydantic", "spec": ">=2,<3"}]
    diff = _compute(reqs, **_PKG)
    # jupyter_server range changed → present; pydantic unchanged → deduped out.
    assert diff["jupyter_server"] == {
        "old": ">=2.4.0,<3", "new": ">=2.5.0,<3", "resolved": True}
    assert "pydantic" not in diff


def test_compute_diff_dedupes_unchanged(recipe_file):
    # Every range identical to the recipe → empty diff (nothing to change).
    reqs = [{"name": "jupyter-server", "spec": ">=2.4.0,<3"},
            {"name": "pydantic", "spec": ">=2,<3"}]
    assert _compute(reqs, **_PKG) == {}


def test_compute_diff_flags_new_unresolved_dep(recipe_file, monkeypatch):
    from superreleaser import condaforge
    monkeypatch.setattr(condaforge, "resolve_conda_name", lambda n: None)
    reqs = [{"name": "totally-new-dep", "spec": ">=1"}]
    diff = _compute(reqs, **_PKG)
    assert diff["totally-new-dep"] == {"old": None, "new": ">=1", "resolved": False}
