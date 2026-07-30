"""Unit tests for the jupyter-ai dependency-range editing (no network)."""

from __future__ import annotations

import pytest

from superreleaser.pyproject import (
    RangeError, apply_ranges, bump_floor, dist_key, missing_from, new_spec,
)

# A faithful excerpt of jupyter-ai's pyproject: underscored dist names, a package
# repeated across two extras, and a package with a differently-shaped ceiling.
PYPROJECT = '''\
[project]
name = "jupyter_ai"
dependencies = [
  "jupyterlab_chat>=0.23.0,<0.24.0",
  "jupyter_ai_router>=0.0.5,<0.1.0",
  "jupyter_server_mcp>=0.2.1,<0.4.0",
  "jupyterlab_commands_toolkit>=0.1.6,<0.2.0",
]

[project.optional-dependencies]
magics = ["jupyter_ai_litellm>=0.0.2,<0.1.0", "jupyter_ai_magic_commands>=0.0.3,<0.1.0"]
jupyternaut = ["jupyter_ai_litellm>=0.0.2,<0.1.0", "jupyter_ai_jupyternaut>=0.1.0b0,<0.2.0"]
docs = ["sphinx", "myst_parser"]
'''


def test_dist_key_normalizes_separators():
    assert dist_key("jupyter_ai_router") == dist_key("jupyter-ai-router")
    assert dist_key("Jupyter.AI_Router") == "jupyter-ai-router"


class TestBumpFloor:
    def test_raises_floor_and_keeps_ceiling(self):
        assert bump_floor(">=0.0.5,<0.1.0", "0.0.6") == ">=0.0.6,<0.1.0"

    def test_keeps_clause_order(self):
        # The ceiling stays where it was, so the diff is minimal.
        assert bump_floor(">=1.0,<2.0", "1.5") == ">=1.5,<2.0"

    def test_adds_floor_when_absent(self):
        assert bump_floor("<2.0", "1.5") == ">=1.5,<2.0"

    def test_empty_spec_gets_a_floor(self):
        assert bump_floor("", "1.2.3") == ">=1.2.3"

    def test_prerelease_floor(self):
        assert bump_floor(">=0.1.0b0,<0.2.0", "0.1.0b2") == ">=0.1.0b2,<0.2.0"

    def test_fails_when_floor_meets_ceiling(self):
        # Releasing 0.1.0 under a `<0.1.0` ceiling can't be fixed by a floor
        # bump — the range would exclude the released version.
        with pytest.raises(RangeError, match="conflicts with existing ceiling"):
            bump_floor(">=0.0.5,<0.1.0", "0.1.0")

    def test_fails_when_floor_exceeds_ceiling(self):
        with pytest.raises(RangeError):
            bump_floor(">=0.0.5,<0.1.0", "0.2.0")

    def test_inclusive_ceiling_boundary_is_allowed(self):
        assert bump_floor(">=1.0,<=2.0", "2.0") == ">=2.0,<=2.0"

    def test_unparseable_ceiling_does_not_block(self):
        assert bump_floor(">=1.0,<not-a-version", "1.5") == ">=1.5,<not-a-version"

    def test_invalid_version_rejected(self):
        with pytest.raises(RangeError, match="invalid version"):
            bump_floor(">=1.0", "not-a-version")


class TestNewSpec:
    def test_auto_bumps_floor(self):
        assert new_spec(">=0.0.5,<0.1.0", "0.0.6", "auto") == ">=0.0.6,<0.1.0"

    def test_missing_range_defaults_to_auto(self):
        assert new_spec(">=0.0.5,<0.1.0", "0.0.6", None) == ">=0.0.6,<0.1.0"

    def test_explicit_range_used_verbatim(self):
        assert new_spec(">=0.0.5,<0.1.0", "0.3.0", ">=0.3.0,<0.4.0") == ">=0.3.0,<0.4.0"

    def test_explicit_range_bypasses_the_ceiling_check(self):
        # This is the documented escape hatch for a breaking bump that `auto`
        # rightly refuses.
        assert new_spec(">=0.0.5,<0.1.0", "0.1.0", ">=0.1.0,<0.2.0") == ">=0.1.0,<0.2.0"


class TestApplyRanges:
    def test_bumps_only_named_packages(self):
        out, changes = apply_ranges(
            PYPROJECT, {"jupyter-ai-router": {"version": "0.0.6", "range": "auto"}}
        )
        assert '"jupyter_ai_router>=0.0.6,<0.1.0"' in out
        # Everything else is untouched.
        assert '"jupyterlab_chat>=0.23.0,<0.24.0"' in out
        assert '"jupyter_server_mcp>=0.2.1,<0.4.0"' in out
        assert changes == {"jupyter-ai-router": ">=0.0.5,<0.1.0 → >=0.0.6,<0.1.0"}

    def test_matches_hyphenated_input_against_underscored_pyproject(self):
        out, _ = apply_ranges(
            PYPROJECT, {"jupyterlab-commands-toolkit": {"version": "0.1.7"}}
        )
        assert '"jupyterlab_commands_toolkit>=0.1.7,<0.2.0"' in out

    def test_bumps_optional_dependencies_in_every_extra(self):
        # jupyter_ai_litellm appears in BOTH magics and jupyternaut.
        out, changes = apply_ranges(
            PYPROJECT, {"jupyter-ai-litellm": {"version": "0.0.3"}}
        )
        assert out.count('"jupyter_ai_litellm>=0.0.3,<0.1.0"') == 2
        assert "jupyter-ai-litellm" in changes

    def test_explicit_range_applied(self):
        out, changes = apply_ranges(
            PYPROJECT,
            {"jupyter-server-mcp": {"version": "0.4.0", "range": ">=0.4.0,<0.5.0"}},
        )
        assert '"jupyter_server_mcp>=0.4.0,<0.5.0"' in out
        assert changes["jupyter-server-mcp"] == ">=0.2.1,<0.4.0 → >=0.4.0,<0.5.0"

    def test_unversioned_requirements_are_left_alone(self):
        # "sphinx" has no spec and isn't in `wanted`; it must survive verbatim.
        out, _ = apply_ranges(PYPROJECT, {"jupyter-ai-router": {"version": "0.0.6"}})
        assert '"sphinx"' in out and '"myst_parser"' in out

    def test_no_change_is_not_reported(self):
        out, changes = apply_ranges(
            PYPROJECT, {"jupyter-ai-router": {"version": "0.0.5"}}
        )
        assert out == PYPROJECT
        assert changes == {}

    def test_breaking_auto_bump_fails_the_whole_edit(self):
        with pytest.raises(RangeError, match="jupyter_ai_router"):
            apply_ranges(
                PYPROJECT, {"jupyter-ai-router": {"version": "0.1.0", "range": "auto"}}
            )

    def test_multiple_packages_at_once(self):
        out, changes = apply_ranges(PYPROJECT, {
            "jupyterlab-chat": {"version": "0.23.1"},
            "jupyter-ai-router": {"version": "0.0.6"},
            "jupyter-ai-magic-commands": {"version": "0.0.4", "range": ">=0.0.4,<0.2.0"},
        })
        assert '"jupyterlab_chat>=0.23.1,<0.24.0"' in out
        assert '"jupyter_ai_router>=0.0.6,<0.1.0"' in out
        assert '"jupyter_ai_magic_commands>=0.0.4,<0.2.0"' in out
        assert len(changes) == 3


class TestMissingFrom:
    def test_detects_a_package_absent_from_pyproject(self):
        assert missing_from(PYPROJECT, {"not-a-real-package": {}}) == ["not-a-real-package"]

    def test_present_packages_are_not_missing(self):
        assert missing_from(PYPROJECT, {"jupyter-ai-router": {}}) == []

    def test_optional_dependency_counts_as_present(self):
        assert missing_from(PYPROJECT, {"jupyter-ai-litellm": {}}) == []
