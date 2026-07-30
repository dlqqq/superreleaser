"""Static configuration for the release DAGs.

Data only — no secrets. `gh` auth comes from the shell. Repos are cloned locally
under these roots (at the repo root by default, gitignored), not assumed to
pre-exist anywhere.
"""

from __future__ import annotations

import os
from pathlib import Path

# Repo root = two levels up from this file (superreleaser/config.py).
_REPO_ROOT = Path(__file__).resolve().parent.parent

FEEDSTOCKS_ROOT = Path(
    os.environ.get("SUPERRELEASER_FEEDSTOCKS_ROOT", str(_REPO_ROOT / "feedstocks"))
)

# Local clones of *source* repos (as opposed to feedstocks). Only the releases
# that edit a repo's own files need one — `simple_jai_release` rewrites
# jupyter-ai's dependency ranges. The single-package DAGs never clone a source
# repo; they drive those releases entirely through `gh`.
SOURCES_ROOT = Path(
    os.environ.get("SUPERRELEASER_SOURCES_ROOT", str(_REPO_ROOT / "sources"))
)


def source_dir(repo: str) -> Path:
    """Local clone dir for a source repo, keyed by its name (`owner/name` → name)."""
    return SOURCES_ROOT / repo.split("/")[-1]


def feedstock_dir(feedstock_name: str) -> Path:
    """Local clone dir for a feedstock, keyed by its repo name (e.g.
    `jupyter-ai-acp-client-feedstock`). Feedstock names aren't derivable from the
    package name (see the registry), so callers pass the resolved name."""
    return FEEDSTOCKS_ROOT / feedstock_name


def recipe_path(feedstock_name: str) -> Path:
    return feedstock_dir(feedstock_name) / "recipe" / "recipe.yaml"
