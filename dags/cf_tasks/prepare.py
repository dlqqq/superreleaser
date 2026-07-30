"""The `prepare` task group for a conda-forge release.

Given an `ident` (from `identity`, resolving the package + version), this group
gets the feedstock ready and works out what the recipe change should be — without
touching anything remote-destructive (no PR, no push of a release branch), so no
dry-run flag is needed here.

Steps:

  1. clone_feedstock  — clone conda-forge/<pkg>-feedstock locally (idempotent)
  2. ensure_fork      — make sure a `fork` remote exists (CF PRs come from a fork)
  3. get_new_reqs     — fetch the released sdist's runtime deps from PyPI
  4. compute_req_diff — {dep: {old, new}} of run-requirement ranges (Python task)
  5. verify_conda     — every mapped dep exists on conda-forge (via `conda search`)

The group owns its own internal XCom wiring; the DAG only needs to instantiate it.

Design notes:
- The git/gh/curl I/O steps are BashOperators running scripts in scripts/*.sh
  (so the exact commands show in the task's rendered template, with automatic
  bash logging). Values reach the scripts via `env=` (append_env=True so the
  inherited environment — SSH_AUTH_SOCK, PATH — is preserved).
- compute_req_diff is application-layer logic (read recipe.yaml, resolve conda
  names, compare ranges), so it's a plain Python @task, not a shell script.
- An output_processor receives only the LAST stdout line, so the JSON-emitting
  scripts print one compact line (jq -c).
- Bodies are tiny and pure, so they're unit-testable without Airflow.
"""

from __future__ import annotations

import json
import re

from airflow.sdk import task, task_group
from airflow.providers.standard.operators.bash import BashOperator

from ._common import BASE, env_from
from superreleaser import condaforge, config, recipe as rcp


# --------------------------------------------------------------------------- #
# output_processor callables — parse a command's stdout into an XCom value.
# --------------------------------------------------------------------------- #
def _proc_pypi_requirements(output: str) -> list[dict]:
    """`curl …/pypi/<pkg>/<ver>/json` stdout → list of {name, spec} runtime deps,
    extras/markers dropped (reuses the tested parser)."""
    data = json.loads(output)
    out = []
    for req in data["info"].get("requires_dist") or []:
        if "extra ==" in req:
            continue
        head = req.split(";")[0].strip()
        m = re.match(r"^([A-Za-z0-9_.\-]+)\s*(.*)$", head)
        if not m:
            continue
        out.append({"name": m.group(1),
                    "spec": rcp._sort_spec(m.group(2).strip().strip("()").strip())})
    return out


# The bash for each step lives in scripts/<task>.sh, Jinja-rendered by Airflow.
# Values are passed via `env=` (safe — no shell interpolation of params/XComs);
# the scripts read them as $PACKAGE, $VERSION, etc. `SCRIPTS` (above) is added to
# the DAG's template_searchpath so bash_command="<name>.sh" resolves. NOTE: an
# output_processor receives only the LAST stdout line, so the JSON-emitting
# scripts print one compact line (jq -c).
@task_group(group_id="prepare", group_display_name="Prepare")
def prepare(ident):
    """Clone + fork the feedstock and compute the recipe dependency change.

    Returns (diff, verify_conda, clone_feedstock): the dependency-diff XComArg
    for downstream consumption, the verify_conda task (so the caller can order
    the next phase after dep verification), and clone_feedstock (the entry task,
    so a caller can gate the whole conda-forge group's start)."""
    env = env_from(ident)

    # 1. Clone the feedstock locally (idempotent: skip if already present).
    clone_feedstock = BashOperator(
        task_id="clone_feedstock",
        bash_command="clone_feedstock.sh",
        env=env,
        **BASE,
        doc_md="Clone `conda-forge/<package>-feedstock` under the local "
        "`feedstocks/` dir (idempotent — fetches if already present).",
    )

    # 2. Ensure a `fork` remote exists — conda-forge PRs are pushed from a fork.
    ensure_fork = BashOperator(
        task_id="ensure_fork",
        bash_command="ensure_fork.sh",
        env=env,
        **BASE,
        doc_md="Ensure a `fork` remote exists on the local feedstock clone "
        "(conda-forge PRs must be published from a fork).",
    )

    # 3. Fetch the released package's declared runtime deps from PyPI.
    get_new_reqs = BashOperator(
        task_id="get_new_reqs",
        bash_command="get_new_reqs.sh",
        env=env,
        **BASE,
        output_processor=_proc_pypi_requirements,
        doc_md="Fetch the released sdist's `requires_dist` from PyPI and parse "
        "the runtime dependencies (extras/markers dropped).",
    )

    # 4. Compute the run-requirement diff. This is application-layer logic (YAML
    #    read + conda-name resolution + spec sorting), so it's a plain Python
    #    task rather than a shell script.
    diff = compute_req_diff(get_new_reqs.output, ident)

    # 5. Verify every dependency in the diff exists on the conda-forge channel.
    #    The diff is serialized by a task rather than ti.xcom_pull (whose
    #    task_id would change with the group's nesting) or .map (which yields a
    #    lazy _MapResult that a BashOperator env= can't consume).
    verify_conda = BashOperator(
        task_id="verify_conda",
        bash_command="verify_conda.sh",
        env={"DIFF": diff_json(diff)},
        **BASE,
        doc_md="For each dependency in the diff, `conda search` conda-forge to "
        "confirm the package exists; fail listing any misses so a human resolves "
        "the name before a PR is opened.",
    )

    clone_feedstock >> ensure_fork >> get_new_reqs >> diff >> verify_conda
    # verify_conda (gates the next phase), the diff XComArg, and clone_feedstock
    # (the entry task, so a caller can gate the whole conda-forge group's start).
    return diff, verify_conda, clone_feedstock


@task(task_id="diff_json", task_display_name="Serialize diff")
def diff_json(diff: dict) -> str:
    """The diff as a compact JSON string, for `verify_conda.sh`'s $DIFF."""
    return json.dumps(diff)


@task(task_id="compute_req_diff")
def compute_req_diff(pypi_reqs: list[dict], ident: dict) -> dict:
    """Diff `requirements.run`: `{dep: {old, new, resolved}}`.

    Pure application-layer logic — reads the cloned recipe.yaml (PyYAML),
    resolves each PyPI dep to its conda-forge name, and compares ranges:
    old = the range currently in the recipe (None for a new dep); new = the
    range from the released package's PyPI metadata; deps whose range is
    unchanged are omitted (a diff shows only changes). `resolved` is False when
    no conda-forge name could be found (flagged downstream, never guessed).
    """
    recipe_text = config.recipe_path(ident["FEEDSTOCK_NAME"]).read_text()
    existing = rcp.current_run_requirements(recipe_text)
    existing_names = {rcp._entry_name(e) for e in existing}
    old_by_conda = {rcp._entry_name(e): " ".join(e.split()[1:]) for e in existing}

    diff: dict[str, dict] = {}
    for req in pypi_reqs:
        dep, spec = req["name"], req["spec"]
        conda = rcp._pypi_to_conda_via_existing(dep, existing_names) \
            or condaforge.resolve_conda_name(dep)
        key = conda or dep
        old = old_by_conda.get(key)
        if old == spec:  # unchanged → not part of the diff
            continue
        diff[key] = {"old": old, "new": spec, "resolved": conda is not None}
    return diff
