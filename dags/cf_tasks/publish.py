"""The `publish` task group: merge the PR and await conda-forge availability.

Runs only after CI is green AND the human approved (the DAG wires those gates
upstream of this group). This is where the release actually ships.

  1. merge             — squash-merge the PR as "<pkg> v<version> (#N)"
  2. await_conda_forge — poll conda-forge (`conda search`, every 60s) until the
                        released version is downloadable
"""

from __future__ import annotations

import subprocess

from airflow.sdk import task, task_group
from airflow.providers.standard.operators.bash import BashOperator

from ._common import BASE, env_from


# A @task.sensor (Python) rather than a BashSensor: BashSensor runs its
# bash_command literally (no template_searchpath) and has no append_env, so
# passing env= would REPLACE the environment and drop PATH (conda not found).
# The old workaround — Jinja-templating `params.package` into the command —
# can't work inside a mapped group, where the package comes from the mapped
# item, not params. A Python sensor takes the value as a plain argument and
# inherits the environment, so it works in both cases.
@task.sensor(
    task_id="await_conda_forge",
    task_display_name="Await conda-forge availability",
    poke_interval=60,
    mode="reschedule",
    timeout=60 * 60 * 2,
)
def await_conda_forge(cf_pkg_name: str, version: str) -> bool:
    """Poke conda-forge until `<package>==<version>` is downloadable.

    `conda search` exits 0 when the exact version is on the channel, non-zero
    while it's still propagating.
    """
    spec = f"{cf_pkg_name}=={version}"
    done = subprocess.run(
        ["conda", "search", "-c", "conda-forge", spec],
        capture_output=True, text=True,
    ).returncode == 0
    print(f"conda search -c conda-forge '{spec}' → "
          f"{'available' if done else 'not yet available'}")
    return done


@task_group(group_id="publish", group_display_name="Publish on Conda Forge")
def publish(ident, pr_url):
    """Merge the approved PR and wait for it to appear on conda-forge."""
    merge = BashOperator(
        task_id="merge",
        task_display_name="Merge PR",
        bash_command="merge.sh",
        env=env_from(ident, PR_URL=pr_url),
        **BASE,
        doc_md="Squash-merge the feedstock PR with commit subject "
        "`<pkg> v<version> (#N)`.",
    )

    awaited = await_conda_forge(ident["CF_PKG_NAME"], ident["VERSION"])

    merge >> awaited
    return {"merge": merge, "await_conda_forge": awaited}
