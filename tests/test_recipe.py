"""Unit tests for the deterministic recipe/version/dep-mapping logic.

These cover the risky, non-obvious parts (recipe editing, version selection,
and the reuse-existing-conda-name rule) with NO network — network-touching
helpers (conda-forge probes, PyPI fetches) are exercised separately/manually.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from superreleaser import recipe as rcp  # noqa: E402


SAMPLE = """\
context:
  version: "0.2.0"

package:
  name: jupyter-ai-acp-client
  version: ${{ version }}

source:
  url: https://pypi.org/packages/source/j/jupyter-ai-acp-client/jupyter_ai_acp_client-${{ version }}.tar.gz
  sha256: 03b3c7a58322a32b31104184bdf4bda06dc07d0cd5d31ae054c21b35ec618b58

build:
  noarch: python
  number: 3

requirements:
  host:
    - python ${{ python_min }}.*
    - pip
  run:
    - python >=${{ python_min }}
    - jupyter_server >=2.4.0,<3
    - jupyterlab-chat >=0.23.0
"""


def test_read_fields():
    assert rcp.current_version(SAMPLE) == "0.2.0"
    assert rcp.pypi_name(SAMPLE) == "jupyter-ai-acp-client"
    assert rcp.conda_package_name(SAMPLE) == "jupyter-ai-acp-client"
    run = rcp.current_run_requirements(SAMPLE)
    assert "jupyter_server >=2.4.0,<3" in run
    assert "jupyterlab-chat >=0.23.0" in run


def test_is_published_matches_exact_version(monkeypatch):
    """is_published bypasses the cache and matches the exact version, so it can
    poll for propagation after a green post-merge build."""
    from superreleaser import condaforge

    monkeypatch.setattr(
        condaforge, "_get_json",
        lambda url: {"versions": ["0.1.5", "0.2.0"]},
    )
    assert condaforge.is_published("jupyter-ai-acp-client", "0.2.0") is True
    assert condaforge.is_published("jupyter-ai-acp-client", "v0.2.0") is True  # v-prefix ok
    assert condaforge.is_published("jupyter-ai-acp-client", "0.2.1") is False


def test_apply_update_preserves_templating_and_bumps():
    new = rcp.apply_update(
        SAMPLE, "0.3.0", "a" * 64,
        ["python >=${{ python_min }}", "jupyter_server >=2.5.0,<3"],
    )
    assert 'version: "0.3.0"' in new
    assert "version: ${{ version }}" in new  # package.version templating intact
    assert "sha256: " + "a" * 64 in new
    assert "number: 0" in new  # build number reset
    assert "- jupyter_server >=2.5.0,<3" in new
    # the dropped dep is gone from run
    assert "jupyterlab-chat" not in new.split("run:")[1]


def test_reuse_existing_conda_name_underscore(monkeypatch):
    """A dep already in the recipe as `jupyter_server` (underscore) must keep
    that name — NOT be re-resolved/hyphenated — when its PyPI name is
    `jupyter-server`."""
    from superreleaser import condaforge

    # PyPI reports the hyphenated dep name; recipe has the underscore conda name.
    monkeypatch.setattr(
        condaforge, "runtime_requirements",
        lambda *_: [("jupyter-server", ">=2.4.0,<3")],
    )
    # If code tried to resolve it as new, this would fire — it must NOT.
    monkeypatch.setattr(
        condaforge, "resolve_conda_name",
        lambda n: (_ for _ in ()).throw(AssertionError("should reuse existing name")),
    )
    monkeypatch.setattr(condaforge, "version_available", lambda *_: True)

    existing = ["python >=${{ python_min }}", "jupyter_server >=2.4.0,<3"]
    m = rcp.map_dependencies("jupyter-ai-acp-client", "0.2.0", existing)
    assert "jupyter_server >=2.4.0,<3" in m["run"]
    assert m["unresolved"] == []


def test_new_dep_unresolved_is_tracked_not_guessed(monkeypatch):
    """A brand-new dep with no conda-forge package is tracked as unresolved and
    NOT added to run (drives the draft-PR + comment path)."""
    from superreleaser import condaforge

    monkeypatch.setattr(
        condaforge, "runtime_requirements",
        lambda *_: [("brand-new-thing", ">=1.0")],
    )
    monkeypatch.setattr(condaforge, "resolve_conda_name", lambda n: None)

    m = rcp.map_dependencies("jupyter-ai-acp-client", "0.2.0", ["python >=3.10"])
    assert m["unresolved"] == ["brand-new-thing"]
    assert not any("brand-new-thing" in e for e in m["run"])


def test_sort_spec_floor_before_ceiling():
    assert rcp._sort_spec("<0.12.0,>=0.11.0") == ">=0.11.0,<0.12.0"
    assert rcp._sort_spec("<3,>=2") == ">=2,<3"
    assert rcp._sort_spec(">=0.23.0a4") == ">=0.23.0a4"  # single clause unchanged
    assert rcp._sort_spec("") == ""


def test_map_dependencies_sorts_specs(monkeypatch):
    from superreleaser import condaforge

    monkeypatch.setattr(
        condaforge, "runtime_requirements",
        lambda *_: [("pydantic", "<3,>=2")],
    )
    m = rcp.map_dependencies("x", "1.0", ["python >=3.10", "pydantic <3,>=2"])
    assert "pydantic >=2,<3" in m["run"]  # floor-first


def test_verify_run_flags_unsatisfiable_range(monkeypatch):
    from superreleaser import condaforge

    def fake_available(name, spec):
        return not spec.startswith(">=99")  # the impossible range fails

    monkeypatch.setattr(condaforge, "version_available", fake_available)
    run = ["python >=${{ python_min }}", "pydantic >=2,<3", "pkgx >=99"]
    unsat = rcp.verify_run(run)
    assert unsat == ["pkgx >=99"]  # python + template skipped, good range passes


def test_earliest_missing_stable_skips_prereleases(monkeypatch):
    monkeypatch.setattr(
        rcp, "_all_pypi_versions",
        lambda _: ["0.1.5", "0.2.0rc0", "0.2.0", "0.2.1", "0.3.0a1"],
    )
    # From 0.1.5 the earliest missing STABLE is 0.2.0 (rc skipped).
    assert rcp.earliest_missing_stable("x", "0.1.5") == "0.2.0"
    # From 0.2.1, only prereleases remain → caught up.
    assert rcp.earliest_missing_stable("x", "0.2.1") is None
