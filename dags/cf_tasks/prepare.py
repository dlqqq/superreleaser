"""The `prepare` task group for a conda-forge release.

Given a `package` and an explicit `version` (from the DAG run params), this group
gets the feedstock ready and works out what the recipe change should be — without
touching anything remote-destructive (no PR, no push of a release branch), so no
dry-run flag is needed here.

Steps (each a small BashOperator; `output_processor` parses stdout into XCom):

  1. clone_feedstock   — clone conda-forge/<pkg>-feedstock locally (idempotent)
  2. ensure_fork       — make sure a `fork` remote exists (CF PRs come from a fork)
  3. check_version     — fail unless the feedstock's current version < proposed
  4. pypi_requirements — fetch the released sdist's runtime deps from PyPI
  5. dependency_diff   — {dep: {old, new}} of run-requirement ranges
  6. verify_conda      — every mapped dep exists on the conda-forge channel

The group owns its own internal XCom wiring; the DAG only needs to instantiate it.

Design notes:
- BashOperator does the I/O (git/gh/curl) so we get its automatic command logging;
  the `output_processor` functions parse stdout and reuse the tested logic in
  `superreleaser.condaforge` / `.recipe` (name resolution, spec sorting) rather
  than reimplementing the subtle hyphen/underscore rules in jq.
- Bodies are tiny and pure, so they're unit-testable without Airflow.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from airflow.sdk import task_group
from airflow.sdk.exceptions import AirflowFailException
from airflow.providers.standard.operators.bash import BashOperator

# Make the `superreleaser` package (repo root) importable.
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from superreleaser import condaforge, config, recipe as rcp  # noqa: E402

_FEEDSTOCKS = str(config.FEEDSTOCKS_ROOT)


# --------------------------------------------------------------------------- #
# output_processor callables — parse a command's stdout into an XCom value.
# --------------------------------------------------------------------------- #
def _proc_current_version(output: str) -> str:
    """Parse `context.version` out of the cloned recipe.yaml (printed by step 3's
    command as the whole file)."""
    return rcp.current_version(output)


def _proc_pypi_requirements(output: str) -> list[dict]:
    """`curl …/pypi/<pkg>/<ver>/json` stdout → list of {name, spec} runtime deps,
    extras/markers dropped (reuses the tested parser)."""
    data = json.loads(output)
    out = []
    for req in data["info"].get("requires_dist") or []:
        if "extra ==" in req:
            continue
        import re
        head = req.split(";")[0].strip()
        m = re.match(r"^([A-Za-z0-9_.\-]+)\s*(.*)$", head)
        if not m:
            continue
        out.append({"name": m.group(1),
                    "spec": rcp._sort_spec(m.group(2).strip().strip("()").strip())})
    return out


@task_group(group_id="prepare")
def prepare():
    """Clone + fork the feedstock, validate the version bump, and compute the
    recipe dependency change. Returns the task group so the DAG can chain it."""

    # 1. Clone the feedstock locally (idempotent: skip if already present).
    clone_feedstock = BashOperator(
        task_id="clone_feedstock",
        bash_command=(
            'set -euo pipefail\n'
            f'mkdir -p "{_FEEDSTOCKS}"\n'
            'pkg="{{ params.package }}"\n'
            f'dir="{_FEEDSTOCKS}/${{pkg}}-feedstock"\n'
            'if [ -d "$dir/.git" ]; then\n'
            '  echo "already cloned: $dir"; git -C "$dir" fetch origin --quiet\n'
            'else\n'
            '  gh repo clone "conda-forge/${pkg}-feedstock" "$dir"\n'
            'fi\n'
            'echo "$dir"'
        ),
        doc_md="Clone `conda-forge/<package>-feedstock` under the local "
        "`feedstocks/` dir (idempotent — fetches if already present).",
    )

    # 2. Ensure a `fork` remote exists — conda-forge PRs are pushed from a fork.
    ensure_fork = BashOperator(
        task_id="ensure_fork",
        bash_command=(
            'set -euo pipefail\n'
            'pkg="{{ params.package }}"\n'
            f'cd "{_FEEDSTOCKS}/${{pkg}}-feedstock"\n'
            'if git remote get-url fork >/dev/null 2>&1; then\n'
            '  echo "fork remote already set"\n'
            'else\n'
            '  echo "Forking ${pkg}-feedstock..."\n'
            '  gh repo fork --remote --remote-name fork\n'
            '  gh repo set-default "$(git remote get-url origin | sed \'s|.*github.com[:/]||;s|\\.git$||\')"\n'
            'fi'
        ),
        doc_md="Ensure a `fork` remote exists on the local feedstock clone "
        "(conda-forge PRs must be published from a fork).",
    )

    # NOTE: a BashOperator's output_processor receives only the LAST line of
    # stdout (SubprocessHook). So every task that feeds a processor emits ONE
    # compact JSON line (via jq -c); processors then read any files they need
    # (the recipe) directly, reusing the tested Python helpers.

    # 3. Guard: the feedstock's current version must be < the proposed version.
    check_version = BashOperator(
        task_id="check_version",
        bash_command=(
            'set -euo pipefail\n'
            'pkg="{{ params.package }}"\n'
            f'recipe="{_FEEDSTOCKS}/${{pkg}}-feedstock/recipe/recipe.yaml"\n'
            'jq -cn --arg proposed "{{ params.version }}" --arg recipe "$recipe" '
            "'{proposed:$proposed, recipe:$recipe}'"
        ),
        output_processor=_guard_version,
        doc_md="Read the feedstock's current recipe version and fail unless it "
        "is **less than** the proposed `version` DAG param.",
    )

    # 4. Fetch the released package's declared runtime deps from PyPI. `jq -c`
    #    guarantees a single compact line for the processor.
    pypi_requirements = BashOperator(
        task_id="pypi_requirements",
        bash_command=(
            'set -euo pipefail\n'
            'curl -fsSL "https://pypi.org/pypi/{{ params.package }}/'
            '{{ params.version }}/json" | jq -c .'
        ),
        output_processor=_proc_pypi_requirements,
        doc_md="Fetch the released sdist's `requires_dist` from PyPI and parse "
        "the runtime dependencies (extras/markers dropped).",
    )

    # 5. Compute the run-requirement diff {dep: {old, new}}. Emits one compact
    #    line carrying the PyPI deps (from XCom) + the recipe path; the processor
    #    reads the recipe and does the conda-name resolution + spec sorting.
    dependency_diff = BashOperator(
        task_id="dependency_diff",
        bash_command=(
            'set -euo pipefail\n'
            'pkg="{{ params.package }}"\n'
            f'recipe="{_FEEDSTOCKS}/${{pkg}}-feedstock/recipe/recipe.yaml"\n'
            "jq -cn --argjson reqs '{{ ti.xcom_pull(task_ids=\"prepare.pypi_requirements\") | tojson }}' "
            '--arg recipe "$recipe" '
            "'{reqs:$reqs, recipe:$recipe}'"
        ),
        output_processor=_compute_diff,
        doc_md="Diff `requirements.run`: `{dep: {old, new}}` mapping the old "
        "recipe ranges to the ranges derived from the released package. New "
        "deps get `old: null`; unresolvable conda names are flagged.",
    )

    # 6. Verify every mapped dependency exists on the conda-forge channel.
    verify_conda = BashOperator(
        task_id="verify_conda",
        bash_command="echo '{{ ti.xcom_pull(task_ids=\"prepare.dependency_diff\") | tojson }}'",
        output_processor=_verify_conda,
        doc_md="For each dependency in the diff, confirm a matching conda-forge "
        "package exists; fail listing any that don't (so a human resolves the "
        "name before a PR is opened).",
    )

    clone_feedstock >> ensure_fork >> check_version >> pypi_requirements
    pypi_requirements >> dependency_diff >> verify_conda


# --------------------------------------------------------------------------- #
# Helpers used by output_processors (kept module-level for unit testing).
# --------------------------------------------------------------------------- #
def _guard_version(output: str) -> str:
    """output = compact JSON `{proposed, recipe}` (recipe = path to recipe.yaml).
    Fail unless current recipe version < proposed; return the proposed version."""
    from packaging.version import Version
    payload = json.loads(output)
    proposed = payload["proposed"].lstrip("v")
    cur = rcp.current_version(Path(payload["recipe"]).read_text())
    if not Version(cur) < Version(proposed):
        raise AirflowFailException(
            f"proposed version {proposed} is not greater than feedstock "
            f"version {cur}"
        )
    return proposed


def _compute_diff(output: str) -> dict:
    """output = compact JSON `{reqs, recipe}` where reqs is the PyPI-deps list
    and recipe is the path to recipe.yaml.

    Returns {dep: {old, new, resolved}} of conda-forge run-requirement ranges:
    old = the range currently in the recipe (None for a new dep); new = the
    range from the released package's PyPI metadata, keyed by the resolved
    conda-forge name.
    """
    payload = json.loads(output)
    pypi_reqs = payload["reqs"]
    existing = rcp.current_run_requirements(Path(payload["recipe"]).read_text())
    existing_names = {rcp._entry_name(e) for e in existing}
    old_by_conda = {
        rcp._entry_name(e): " ".join(e.split()[1:]) for e in existing
    }

    diff: dict[str, dict] = {}
    for req in pypi_reqs:
        dep, spec = req["name"], req["spec"]
        conda = rcp._pypi_to_conda_via_existing(dep, existing_names) \
            or condaforge.resolve_conda_name(dep)
        key = conda or dep
        diff[key] = {
            "old": old_by_conda.get(key),
            "new": spec,
            "resolved": conda is not None,
        }
    return diff


def _verify_conda(diff_json: str) -> dict:
    """Confirm each dependency name exists on conda-forge; fail listing misses."""
    diff = json.loads(diff_json)
    missing = [name for name, d in diff.items()
               if not d.get("resolved") or condaforge.cf_package(name) is None]
    if missing:
        raise AirflowFailException(
            "no conda-forge package found for: " + ", ".join(sorted(missing))
        )
    return diff
