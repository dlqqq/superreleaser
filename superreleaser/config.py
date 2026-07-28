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


def feedstock_repo(package: str) -> str:
    """The conda-forge feedstock GitHub repo for a package (org/name)."""
    return f"conda-forge/{package}-feedstock"

# Default guardrail. Real by default per the decided model ("Real PR, no
# merge"): edit the recipe, push a branch, open a real feedstock PR + comment,
# but NEVER auto-merge. Flip to dry-run to exercise the DAG shape without
# pushing anything (branch/push/PR/comment are printed instead).
DRY_RUN_DEFAULT = os.environ.get("SUPERRELEASER_DRY_RUN", "0") != "0"


def feedstock_dir(package: str) -> Path:
    return FEEDSTOCKS_ROOT / f"{package}-feedstock"


def recipe_path(package: str) -> Path:
    return feedstock_dir(package) / "recipe" / "recipe.yaml"
