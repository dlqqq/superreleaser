"""conda-forge release DAG for one Jupyter extension package.

The whole pipeline is the `conda_forge_release` task group (in cf_tasks/); this
DAG just drops it in. Runnable on its own when the version is already on PyPI;
the full GitHub→PyPI→conda-forge flow is `e2e_release`.

Trigger with a package (dropdown) + explicit version:

    { "package": "jupyter-ai-acp-client", "version": "0.2.1" }

Requires Airflow 3.1+ for the HITL ApprovalOperator (built on 3.3.0 here).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pendulum

from airflow.sdk import dag, Param

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cf_tasks._common import MACROS, SCRIPTS, identity
from cf_tasks.conda_forge import conda_forge_release
from superreleaser.registry import PACKAGE_NAMES


@dag(
    dag_id="cf_release",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["superreleaser", "conda-forge"],
    params={
        "package": Param("jupyter-ai-acp-client", type="string", enum=PACKAGE_NAMES),
        "version": Param("", type="string"),
    },
    template_searchpath=[SCRIPTS],
    user_defined_macros=MACROS,  # exposes pkg(name) → registry entry in Jinja
)
def cf_release():
    # `identity` resolves the registry names once; every step downstream reads
    # them from it rather than from params — the same shape the mapped
    # simple_jai_release uses.
    conda_forge_release(identity("{{ params.package }}", "{{ params.version }}"))


cf_release()
