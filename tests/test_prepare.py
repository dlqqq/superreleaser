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
from airflow.sdk.exceptions import AirflowFailException  # noqa: E402


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
    p = tmp_path / "recipe.yaml"
    p.write_text(RECIPE)
    return str(p)


def test_guard_version_accepts_bump(recipe_file):
    out = json.dumps({"proposed": "v0.2.1", "recipe": recipe_file})
    assert prepare._guard_version(out) == "0.2.1"


def test_guard_version_rejects_non_bump(recipe_file):
    for bad in ("0.2.0", "0.1.0"):  # equal or lower
        out = json.dumps({"proposed": bad, "recipe": recipe_file})
        with pytest.raises(AirflowFailException):
            prepare._guard_version(out)


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


def test_compute_diff_maps_conda_names_and_ranges(recipe_file, monkeypatch):
    # jupyter-server (PyPI) must map to the recipe's existing jupyter_server.
    reqs = [{"name": "jupyter-server", "spec": ">=2.5.0,<3"},
            {"name": "pydantic", "spec": ">=2,<3"}]
    out = json.dumps({"reqs": reqs, "recipe": recipe_file})
    diff = prepare._compute_diff(out)
    assert diff["jupyter_server"] == {
        "old": ">=2.4.0,<3", "new": ">=2.5.0,<3", "resolved": True}
    assert diff["pydantic"]["old"] == ">=2,<3"


def test_compute_diff_flags_new_unresolved_dep(recipe_file, monkeypatch):
    from superreleaser import condaforge
    monkeypatch.setattr(condaforge, "resolve_conda_name", lambda n: None)
    reqs = [{"name": "totally-new-dep", "spec": ">=1"}]
    out = json.dumps({"reqs": reqs, "recipe": recipe_file})
    diff = prepare._compute_diff(out)
    assert diff["totally-new-dep"] == {"old": None, "new": ">=1", "resolved": False}


def test_verify_conda_raises_on_missing(monkeypatch):
    from superreleaser import condaforge
    monkeypatch.setattr(condaforge, "cf_package", lambda n: None)
    diff = json.dumps({"pydantic": {"old": None, "new": ">=2", "resolved": True}})
    with pytest.raises(AirflowFailException):
        prepare._verify_conda(diff)
