"""Static configuration for the conda-forge release DAG.

Data only — no secrets. `gh` auth comes from the shell; the feedstocks live as
git submodules under FEEDSTOCKS_ROOT.
"""

from __future__ import annotations

import os
from pathlib import Path

FEEDSTOCKS_ROOT = Path(
    os.environ.get(
        "SUPERRELEASER_FEEDSTOCKS_ROOT",
        str(Path.home() / "workplace" / "jupyter-ai-feedstocks"),
    )
)

# Default guardrail. Real by default per the decided model ("Real PR, no
# merge"): edit the recipe, push a branch, open a real feedstock PR + comment,
# but NEVER auto-merge. Flip to dry-run to exercise the DAG shape without
# pushing anything (branch/push/PR/comment are printed instead).
DRY_RUN_DEFAULT = os.environ.get("SUPERRELEASER_DRY_RUN", "0") != "0"


def feedstock_dir(package: str) -> Path:
    return FEEDSTOCKS_ROOT / f"{package}-feedstock"


def recipe_path(package: str) -> Path:
    return feedstock_dir(package) / "recipe" / "recipe.yaml"
