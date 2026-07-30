"""Release a wave of Jupyter AI subpackages, then jupyter-ai itself — one DAG run.

Phase A releases every listed subpackage **in parallel** (full E2E: GitHub prep →
PyPI → conda-forge), each with its own two human gates. Phase B then cuts
jupyter-ai: bump its dependency ranges to the versions just published, merge the
release notes, and run the same E2E release for jupyter-ai.

    plan_release ─► to_release ─► release_subpackage.expand(...) ─┐
                                                                  ▼
      bump_ranges ─► release_docs ─► pypi_release ─► conda_forge_release
        (jupyter-ai, sequential — nothing here starts until every
         subpackage is live on both PyPI and conda-forge)

Trigger with the wave plus the jupyter-ai version to cut:

    {
      "subpackages": [
        {"package": "jupyter-ai-router",     "version": "0.0.6"},
        {"package": "jupyter-ai-acp-client", "version": "0.3.0", "range": ">=0.3.0,<0.4.0"}
      ],
      "jupyter_ai_version": "3.2.0"
    }

`range` says what to write into jupyter-ai's pyproject: `"auto"` (the default)
raises the floor to `version` and keeps the existing ceiling; an explicit spec is
used verbatim. A subpackage already published at its version is skipped in Phase A
but still range-bumped in Phase B, so re-running a partially-completed wave is
safe.

Requires Airflow 3.1+ for the HITL ApprovalOperator (built on 3.3.0 here).
"""

from __future__ import annotations

import sys
import urllib.error
import urllib.request
from pathlib import Path

import pendulum

from airflow.sdk import dag, task, task_group, Param

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cf_tasks._common import MACROS, SCRIPTS, identity
from cf_tasks.bump_ranges import bump_ranges
from cf_tasks.conda_forge import conda_forge_release
from cf_tasks.pypi import pypi_release
from cf_tasks.release_docs import release_docs
from superreleaser import registry
from superreleaser.pyproject import AUTO, dist_key

JUPYTER_AI = "jupyter-ai"

# Example wave, shown as the param default so the trigger form is self-documenting.
EXAMPLE_SUBPACKAGES = [{"package": "jupyter-ai-router", "version": "0.0.6",
                        "range": AUTO}]


def _on_pypi(pypi_name: str, version: str) -> bool:
    """Is this exact version already published on PyPI?"""
    url = f"https://pypi.org/pypi/{pypi_name}/{version}/json"
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            return r.status == 200
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
        return False


@task(task_id="plan_release", task_display_name="Plan the release",
      multiple_outputs=True)
def plan_release(subpackages: list[dict], jupyter_ai_version: str) -> dict:
    """Validate the input and split it into "release now" vs "range-bump only".

    Every entry is validated up front (known package, non-empty version) so a typo
    fails here rather than halfway through a wave of real releases. Entries already
    published at their version are dropped from the release list — but they still
    get their range bumped, because the point of Phase B is that jupyter-ai's
    pyproject names the whole wave, however much of it this run actually cut.

    Returns `{"to_release", "wanted", "skipped"}`.
    """
    if not subpackages:
        raise ValueError("`subpackages` is empty — nothing to release")
    if not str(jupyter_ai_version).strip():
        raise ValueError("`jupyter_ai_version` is required")

    to_release, wanted, skipped = [], {}, []
    for entry in subpackages:
        package = str(entry.get("package", "")).strip()
        version = str(entry.get("version", "")).strip().lstrip("v")
        if not package or not version:
            raise ValueError(f"each subpackage needs a package and version: {entry!r}")
        if package == JUPYTER_AI:
            raise ValueError(
                "jupyter-ai is released by Phase B — don't list it in `subpackages` "
                "(use `jupyter_ai_version`)"
            )
        pkg = registry.get(package)  # raises listing the valid names

        wanted[dist_key(package)] = {
            "version": version,
            "range": str(entry.get("range") or AUTO).strip(),
        }
        if _on_pypi(pkg.pypi_name, version):
            print(f"{package} {version} is already on PyPI — skipping its release")
            skipped.append(f"{package} {version}")
        else:
            to_release.append({"package": package, "version": version})

    print(f"releasing {len(to_release)}, bumping ranges for {len(wanted)}")
    return {"to_release": to_release, "wanted": wanted, "skipped": skipped}


@task(task_id="to_release", task_display_name="Subpackages to release")
def to_release(plan: dict) -> list[dict]:
    """The mapped list, as a task's plain return value.

    This exists only because `expand_kwargs()` rejects a keyed XComArg
    (`plan["to_release"]` → "cannot map over XCom with custom key"): it can map
    over a task's `return_value` and nothing else. So the list gets its own task.
    """
    return plan["to_release"]


@task_group(group_id="release_subpackage",
            group_display_name="Release a subpackage")
def release_subpackage(package: str, version: str):
    """Full E2E release of ONE subpackage — the mapped unit of Phase A.

    `package`/`version` arrive as MappedArgument stubs, which is exactly why the
    inner groups take an `ident` rather than reading `params`: each expansion
    resolves its own package. Nested groups, HITL gates, XCom threading and
    sensors all work per map index.
    """
    ident = identity(package, version)
    pypi_done = pypi_release(ident)   # exit = await_pypi
    cf = conda_forge_release(ident)   # {"entry", "exit"}
    pypi_done >> cf["entry"]
    return cf["exit"]


@dag(
    dag_id="simple_jai_release",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["superreleaser", "release", "jupyter-ai"],
    params={
        "subpackages": Param(
            EXAMPLE_SUBPACKAGES,
            type="array",
            description="Subpackages to release: {package, version, range}. "
            "`range` is what to write into jupyter-ai's pyproject — omit or use "
            '"auto" to raise the floor and keep the existing ceiling.',
        ),
        "jupyter_ai_version": Param(
            "", type="string",
            description="The jupyter-ai version to cut after the wave is live.",
        ),
    },
    template_searchpath=[SCRIPTS],
    user_defined_macros=MACROS,
    # A wave of parallel releases is a lot of gh/git/conda work; keep one run at a
    # time so two waves can't fight over the same feedstock clones.
    max_active_runs=1,
)
def simple_jai_release():
    jai_version = "{{ params.jupyter_ai_version }}"
    jai_repo = registry.get(JUPYTER_AI).repo

    # --- Phase A: every subpackage, in parallel ---------------------------- #
    plan = plan_release("{{ params.subpackages }}", jai_version)
    releases = release_subpackage.expand_kwargs(to_release(plan))

    # --- Phase B: jupyter-ai itself ---------------------------------------- #
    # `none_failed` on Phase B's entry task is essential: when every listed
    # subpackage is already published, the mapped group expands to zero instances
    # and is SKIPPED — an all_success downstream would skip too, silently
    # abandoning the jupyter-ai release in exactly the re-run case we support.
    # It's set on the entry task alone (not via default_args, which would also
    # loosen the merge tasks and let them merge a PR that was never opened).
    bump = bump_ranges(jai_repo, jai_version, plan["wanted"],
                       entry_trigger_rule="none_failed")

    # Likewise for Step 0: the bump group short-circuits when nothing needs
    # bumping, and the release must carry on regardless.
    docs = release_docs(jai_repo, jai_version, entry_trigger_rule="none_failed")

    ident = identity.override(task_id="jai_identity",
                              trigger_rule="none_failed")(JUPYTER_AI, jai_version)
    jai_pypi = pypi_release(ident)
    jai_cf = conda_forge_release(ident)

    releases >> bump["entry"]
    bump["exit"] >> docs["entry"]
    docs["exit"] >> ident >> jai_pypi >> jai_cf["entry"]


simple_jai_release()
