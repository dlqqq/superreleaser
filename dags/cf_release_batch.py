"""Batch conda-forge release DAG — release several packages in dependency order.

Orchestrates the single-package `cf_release` DAG once per package, so each
package gets its own full pipeline (recipe update → PR → rerender → CI → human
approval → merge → post-merge build → conda-forge availability) and its own row
in the UI.

Why full-release-per-package, in order (not "prep everything in parallel"):
a dependent package's recipe pins its upstream by version — e.g. `jupyter-ai
3.1.1` runs `jupyter-ai-acp-client >=0.2.1`. `cf_release`'s verify step checks
that range actually resolves ON conda-forge, which only becomes true once
acp-client's release has fully shipped (merged + built + propagated). So an
upstream must be LIVE before a dependent's release starts. That ordering is the
whole point of the topologically-sorted plan.

Trigger with a conf of ordered "waves" — packages within a wave have no
dependency on each other and release concurrently; waves run strictly in
sequence, and a failure/rejection anywhere stops the batch (dependents never
start, since they'd fail verification against an upstream that didn't ship):

  {
    "dry_run": true,
    "waves": [
      [ {"package": "jupyter-ai-acp-client", "version": "0.2.1"} ],
      [ {"package": "jupyter-ai", "version": "3.1.1"} ]
    ]
  }

Implementation: a fixed number of wave stages are declared statically (Airflow
needs the graph at parse time; `dag_run.conf` isn't known until run time). Each
stage reads its wave from the conf and a mapped `TriggerDagRunOperator` expands
over that wave's packages — an absent/empty wave simply maps to nothing and is
skipped. Stages are chained wave_0 >> wave_1 >> …, so waves are sequential while
packages within a wave run in parallel.
"""

from __future__ import annotations

import logging

import pendulum

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

log = logging.getLogger("superreleaser.batch")

# Max waves declarable at parse time. Extra waves in a plan beyond this are
# reported by `validate_plan` rather than silently dropped. Bump if needed.
MAX_WAVES = 6


def _wave_confs(waves: list, index: int, dry_run: bool) -> list[dict]:
    """The list of child-DAG `conf`s for wave `index` (empty if none). Each conf
    releases one package via the single-package cf_release DAG."""
    if index >= len(waves):
        return []
    out = []
    for entry in waves[index]:
        out.append({
            "package": entry["package"],
            "version": entry.get("version", ""),
            "dry_run": dry_run,
        })
    return out


@dag(
    dag_id="cf_release_batch",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["superreleaser", "conda-forge", "batch"],
    params={"waves": [], "dry_run": False},
)
def cf_release_batch():
    @task(
        task_display_name="Validate plan",
        doc_md="Read the `waves` plan from the trigger conf and surface it. "
        f"Fails if the plan has more than {MAX_WAVES} waves (the statically "
        "declared maximum).",
    )
    def validate_plan(**context) -> dict:
        conf = context["dag_run"].conf or {}
        waves = conf.get("waves") or context["params"]["waves"]
        dry_run = bool(conf.get("dry_run", context["params"]["dry_run"]))
        if len(waves) > MAX_WAVES:
            raise ValueError(
                f"plan has {len(waves)} waves but only {MAX_WAVES} are declared; "
                f"raise MAX_WAVES in cf_release_batch.py"
            )
        total = sum(len(w) for w in waves)
        log.info("batch plan: %d wave(s), %d package(s), dry_run=%s",
                 len(waves), total, dry_run)
        for i, w in enumerate(waves):
            log.info("  wave %d: %s", i, [e["package"] for e in w])
        return {"waves": waves, "dry_run": dry_run}

    @task(task_display_name="Plan wave")
    def plan_wave(plan: dict, index: int) -> list[dict]:
        """Child-DAG confs for one wave (empty ⇒ that wave stage is skipped)."""
        return _wave_confs(plan["waves"], index, plan["dry_run"])

    plan = validate_plan()

    # Declare a fixed ladder of wave stages. Each stage maps a
    # TriggerDagRunOperator over its wave's package confs; waves are chained so
    # wave N+1 only starts once every release in wave N has completed. A child
    # that fails or is rejected leaves its run in a failed state, which
    # propagates here (failed_states) and stops the ladder.
    prev = plan
    for i in range(MAX_WAVES):
        confs = plan_wave.override(task_id=f"plan_wave_{i}")(plan, i)
        releases = TriggerDagRunOperator.partial(
            task_id=f"release_wave_{i}",
            trigger_dag_id="cf_release",
            wait_for_completion=True,
            poke_interval=60,
            deferrable=True,           # free the worker slot while the child runs
            failed_states=["failed"],  # a failed/rejected child stops the batch
            reset_dag_run=True,        # allow re-runs with the same logical date
            map_index_template="{{ task.conf['package'] }}",
        ).expand(conf=confs)

        # Sequence: prev stage's releases must finish before this wave plans/runs.
        prev >> confs >> releases
        prev = releases

    return


cf_release_batch()
