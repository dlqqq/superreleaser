"""The `publish` task group: merge the PR and await conda-forge availability.

Runs only after CI is green AND the human approved (the DAG wires those gates
upstream of this group). This is where the release actually ships.

  1. merge             — squash-merge the PR as "<pkg> v<version> (#N)"
  2. await_conda_forge — poll conda-forge (`conda search`, every 60s) until the
                        released version is downloadable
"""

from __future__ import annotations

from airflow.sdk import task_group
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.bash import BashSensor

from ._common import BASE, ENV


@task_group(group_id="publish", group_display_name="Publish on Conda Forge")
def publish(pr_url):
    """Merge the approved PR and wait for it to appear on conda-forge."""
    merge = BashOperator(
        task_id="merge",
        task_display_name="Merge PR",
        bash_command="merge.sh",
        env={**ENV, "PR_URL": pr_url},
        **BASE,
        doc_md="Squash-merge the feedstock PR with commit subject "
        "`<pkg> v<version> (#N)`.",
    )

    # BashSensor runs bash_command literally (no template_searchpath / .sh
    # lookup, unlike BashOperator) and has no append_env — passing env= would
    # REPLACE the environment and drop PATH (conda not found). So inherit the
    # environment (no env=) and Jinja-template the params straight into the
    # command (trusted DAG params, not arbitrary input). `conda search` exits 0
    # when the exact version is on the channel, non-zero to keep polling.
    await_conda_forge = BashSensor(
        task_id="await_conda_forge",
        task_display_name="Await conda-forge availability",
        bash_command="conda search -c conda-forge "
        "'{{ params.package }}=={{ params.version }}' >/dev/null 2>&1",
        poke_interval=60,
        mode="reschedule",
        timeout=60 * 60 * 2,
        doc_md="Poll conda-forge (`conda search`) every 60s until "
        "`<package>==<version>` is downloadable.",
    )

    merge >> await_conda_forge
    return {"merge": merge, "await_conda_forge": await_conda_forge}
