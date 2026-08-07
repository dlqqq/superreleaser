"""End-to-end release DAG for a Jupyter extension package.

Two composable task groups, in sequence:

    Release on PyPI  →  Release on Conda Forge

The first runs the repo's Jupyter Releaser workflows (Step 1 prep → human gate →
Step 2 publish) and waits for PyPI; the second is the entire conda-forge release
(the same group `cf_release` uses). Each group has its own human gate.

Trigger with a package (dropdown) + explicit version:

    { "package": "jupyter-ai-acp-client", "version": "0.2.2" }

Requires Airflow 3.1+ for the HITL ApprovalOperator (built on 3.3.0 here).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pendulum

from airflow.sdk import dag, Param

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cf_tasks._common import MACROS, SCRIPTS
from cf_tasks.conda_forge import conda_forge_release
from cf_tasks.pypi import pypi_release
from superreleaser.registry import PACKAGE_NAMES


@dag(
    dag_id="e2e_release",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["superreleaser", "release"],
    params={
        "package": Param("jupyter-ai-acp-client", type="string", enum=PACKAGE_NAMES),
        "version": Param("", type="string"),
        # Optional branch overrides for backports. Blank = the repo's default
        # branch (jupyter-releaser falls back to it; the feedstock scripts fall
        # back to origin/HEAD). Set these to release off a maintenance branch,
        # e.g. cut a 0.2.x patch while main is on 0.3.x.
        "source_branch": Param(
            "", type="string",
            description="Source-repo branch to run the Jupyter Releaser "
            "workflows against (blank = repo default).",
        ),
        "feedstock_branch": Param(
            "", type="string",
            description="Feedstock branch to base the release branch on and "
            "open the PR against (blank = feedstock default).",
        ),
    },
    template_searchpath=[SCRIPTS],
    user_defined_macros=MACROS,
)
def e2e_release():
    pypi_done = pypi_release()          # returns await_pypi (the group's exit)
    cf = conda_forge_release()          # {"entry", "exit"}
    # conda-forge's first task starts only once the version is live on PyPI.
    pypi_done >> cf["entry"]


e2e_release()
