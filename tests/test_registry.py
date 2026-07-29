"""Sanity checks on the package registry (no network)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from superreleaser import registry  # noqa: E402


def test_every_entry_is_well_formed():
    assert registry.PACKAGES, "registry is empty"
    for name, p in registry.PACKAGES.items():
        assert "/" in p.repo, f"{name}: repo must be owner/name"
        assert p.feedstock_repo.startswith("conda-forge/"), \
            f"{name}: feedstock_repo must be under conda-forge/"
        assert p.feedstock_repo.endswith("-feedstock"), \
            f"{name}: feedstock_repo must end with -feedstock"
        assert p.pypi_name and p.cf_pkg_name


def test_package_names_is_sorted_keys():
    assert registry.PACKAGE_NAMES == sorted(registry.PACKAGES)


def test_get_unknown_raises_with_hint():
    with pytest.raises(KeyError):
        registry.get("not-a-real-package")


def test_known_edge_cases():
    # Repo name differs from the package name.
    assert registry.get("jupyterlab-chat").repo == "jupyterlab/jupyter-chat"
    # Underscores on PyPI + conda-forge.
    jsd = registry.get("jupyter-server-documents")
    assert jsd.pypi_name == "jupyter_server_documents"
    assert jsd.cf_pkg_name == "jupyter_server_documents"
