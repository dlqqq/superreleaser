"""Unit tests for the batch plan → child-DAG conf translation."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))

from cf_release_batch import _wave_confs  # noqa: E402


WAVES = [
    [{"package": "jupyter-ai-acp-client", "version": "0.2.1"}],
    [{"package": "jupyter-ai", "version": "3.1.1"}],
]


def test_wave_confs_maps_each_package():
    assert _wave_confs(WAVES, 0, True) == [
        {"package": "jupyter-ai-acp-client", "version": "0.2.1", "dry_run": True}
    ]
    assert _wave_confs(WAVES, 1, False) == [
        {"package": "jupyter-ai", "version": "3.1.1", "dry_run": False}
    ]


def test_absent_wave_is_empty():
    # Beyond the plan → empty list → that wave stage is skipped, not an error.
    assert _wave_confs(WAVES, 2, True) == []
    assert _wave_confs([], 0, True) == []


def test_version_defaults_to_empty():
    # Missing version → "" so cf_release falls back to earliest-missing-stable.
    assert _wave_confs([[{"package": "x"}]], 0, False) == [
        {"package": "x", "version": "", "dry_run": False}
    ]


def test_wave_with_multiple_packages_all_mapped():
    plan = [[{"package": "a", "version": "1"}, {"package": "b", "version": "2"}]]
    confs = _wave_confs(plan, 0, True)
    assert [c["package"] for c in confs] == ["a", "b"]  # both released in-wave
