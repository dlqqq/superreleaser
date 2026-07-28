"""Test DAG: exercise just the `prepare` task group in isolation.

Trigger with a conf giving the package and the explicit target version, e.g.:

    { "package": "jupyter-ai-acp-client", "version": "0.2.1" }

The prepare group clones + forks the feedstock, checks the version bump, fetches
the PyPI deps, computes the run-requirement diff, and verifies the deps exist on
conda-forge. Nothing remote-destructive happens, so there's no dry-run flag.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pendulum

from airflow.sdk import dag

# Make the sibling cf_tasks/ package importable.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from cf_tasks.prepare import prepare


@dag(
    dag_id="test-cf-release-prep",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["superreleaser", "conda-forge", "test"],
    params={"package": "jupyter-ai-acp-client", "version": "0.2.1"},
)
def test_cf_release_prep():
    prepare()


test_cf_release_prep()
