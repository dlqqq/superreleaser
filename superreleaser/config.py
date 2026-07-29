"""Static configuration for the conda-forge release DAG.

Data only — no secrets. `gh` auth comes from the shell. Feedstocks are cloned
locally under FEEDSTOCKS_ROOT (./feedstocks at the repo root by default,
gitignored), not assumed to pre-exist anywhere.
"""

from __future__ import annotations

import os
from pathlib import Path

# Repo root = two levels up from this file (superreleaser/config.py).
_REPO_ROOT = Path(__file__).resolve().parent.parent

FEEDSTOCKS_ROOT = Path(
    os.environ.get("SUPERRELEASER_FEEDSTOCKS_ROOT", str(_REPO_ROOT / "feedstocks"))
)


def feedstock_dir(feedstock_name: str) -> Path:
    """Local clone dir for a feedstock, keyed by its repo name (e.g.
    `jupyter-ai-acp-client-feedstock`). Feedstock names aren't derivable from the
    package name (see the registry), so callers pass the resolved name."""
    return FEEDSTOCKS_ROOT / feedstock_name


def recipe_path(feedstock_name: str) -> Path:
    return feedstock_dir(feedstock_name) / "recipe" / "recipe.yaml"
