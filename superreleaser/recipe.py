"""Recipe reading/editing + version selection.

Recipe edits are surgical text substitutions (like the feedstocks repo's own
`update_version_and_hash.py`) rather than a full YAML round-trip, to preserve
the `${{ ... }}` templating and comments conda-forge recipes rely on.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml
from packaging.version import InvalidVersion, Version

from . import condaforge


# --------------------------------------------------------------------------- #
# Reading the recipe
#
# conda-forge recipes use `${{ ... }}` templating, but every such value is a
# plain scalar string to a YAML parser, so PyYAML reads them fine. Reading with
# yaml.safe_load is far more robust than regex-scraping the file.
# --------------------------------------------------------------------------- #
def _load(recipe_text: str) -> dict:
    return yaml.safe_load(recipe_text) or {}


def current_version(recipe_text: str) -> str:
    version = _load(recipe_text).get("context", {}).get("version")
    if version is None:
        raise ValueError("could not parse context.version from recipe")
    return str(version)


def pypi_name(recipe_text: str) -> str:
    # Prefer the source URL's project name; fall back to context.name.
    url = _load(recipe_text).get("source", {}).get("url", "")
    m = re.search(r"pypi\.org/packages/source/./([^/${}]+)/", url)
    if m:
        return m.group(1)
    name = _load(recipe_text).get("context", {}).get("name")
    if name:
        return str(name)
    raise ValueError("could not parse PyPI name from recipe")


def current_run_requirements(recipe_text: str) -> list[str]:
    """The existing `requirements.run:` entries, verbatim (their conda names are
    already correct — we reuse them and only refresh version ranges)."""
    return list(_load(recipe_text).get("requirements", {}).get("run", []) or [])


# --------------------------------------------------------------------------- #
# Version selection — earliest missing STABLE, one at a time
# --------------------------------------------------------------------------- #
def earliest_missing_stable(pypi_project: str, feedstock_version: str) -> str | None:
    """The OLDEST stable PyPI version strictly newer than the feedstock's
    current version. Prereleases skipped. None if the feedstock is caught up.
    conda-forge convention is one version bump per PR, so we walk forward one
    step at a time."""
    releases = _all_pypi_versions(pypi_project)
    try:
        base = Version(feedstock_version)
    except InvalidVersion:
        return None
    candidates: list[Version] = []
    for v in releases:
        try:
            ver = Version(v)
        except InvalidVersion:
            continue
        if ver.is_prerelease:
            continue
        if ver > base:
            candidates.append(ver)
    if not candidates:
        return None
    return str(min(candidates))


def _all_pypi_versions(pypi_project: str) -> list[str]:
    import json
    import urllib.request

    with urllib.request.urlopen(
        f"https://pypi.org/pypi/{pypi_project}/json", timeout=15
    ) as r:
        return list(json.loads(r.read()).get("releases", {}).keys())


# --------------------------------------------------------------------------- #
# Dependency mapping — the hard part
# --------------------------------------------------------------------------- #
def _entry_name(entry: str) -> str:
    """`jupyter_server >=2.4.0,<3` -> `jupyter_server`."""
    return entry.strip().split()[0]


def _sort_spec(spec: str) -> str:
    """Order the comma-separated clauses of a version spec floor-first, so a
    reader sees the lower bound before the upper bound.

    `<0.12.0,>=0.11.0` -> `>=0.11.0,<0.12.0`. Clauses are ranked by operator:
    lower bounds (`>=`, `>`, `==`, `~=`) before upper bounds (`<=`, `<`), then
    alphabetically as a stable tiebreaker. Unparseable input is returned as-is.
    """
    if not spec:
        return spec
    order = {">=": 0, ">": 0, "==": 0, "~=": 0, "!=": 1, "<=": 2, "<": 2}

    def rank(clause: str) -> tuple[int, str]:
        clause = clause.strip()
        for op, r in sorted(order.items(), key=lambda kv: -len(kv[0])):
            if clause.startswith(op):
                return (r, clause)
        return (1, clause)

    clauses = [c.strip() for c in spec.split(",") if c.strip()]
    return ",".join(sorted(clauses, key=rank))


def _pypi_to_conda_via_existing(pypi_name_: str, existing_names: set[str]) -> str | None:
    """If a run entry already in the recipe matches this PyPI dep (modulo
    `_`/`-`), reuse that (already-correct) conda name."""
    norm = pypi_name_.lower().replace("_", "-")
    for name in existing_names:
        if name.lower().replace("_", "-") == norm:
            return name
    return None


def map_dependencies(
    pypi_project: str, version: str, existing_run: list[str]
) -> dict:
    """Compute the new `requirements.run` list from the released package's own
    metadata.

    Strategy (probe, never guess):
      1. For each PyPI runtime dep, if it's already a run entry, reuse that
         conda name (correct by construction) and just set the new range.
      2. Otherwise it's a NEW dep — probe conda-forge for its name. Found →
         add it (and verify the range resolves). Not found → record as
         `unresolved` (→ draft PR + comment, no fabricated name).

    Returns:
      {
        "run": [<entry>, ...],          # proposed requirements.run
        "unresolved": [<pypi_name>, ...],  # new deps with no CF package
      }

    Version-range availability is checked separately by `verify_run` (the DAG's
    verify_cf step), keeping name-resolution and range-verification distinct.
    """
    existing_names = {_entry_name(e) for e in existing_run}
    deps = condaforge.runtime_requirements(pypi_project, version)

    run: list[str] = []
    unresolved: list[str] = []

    # python is always a run dep in these recipes but isn't in requires_dist;
    # preserve any existing `python ...` entry verbatim.
    for e in existing_run:
        if _entry_name(e).lower() == "python":
            run.append(e)

    for dep, spec in deps:
        spec = _sort_spec(spec)  # floor before ceiling
        conda = _pypi_to_conda_via_existing(dep, existing_names)
        if conda is None:  # new dep — probe conda-forge, never guess
            conda = condaforge.resolve_conda_name(dep)
        if conda is None:
            unresolved.append(dep)
            continue
        run.append(f"{conda} {spec}".strip())

    return {"run": run, "unresolved": unresolved}


def verify_run(run: list[str]) -> list[str]:
    """For each `requirements.run` entry, verify the conda-forge package exists
    AND its version range resolves to at least one real build. Returns the
    entries that DON'T (the `unsatisfied` list). `python` and template-valued
    entries are skipped (not real conda-forge lookups)."""
    unsatisfied: list[str] = []
    for entry in run:
        name = _entry_name(entry)
        if name.lower() == "python" or "${{" in entry:
            continue
        spec = entry[len(name):].strip()
        if not condaforge.version_available(name, spec):
            unsatisfied.append(entry)
    return unsatisfied


# --------------------------------------------------------------------------- #
# Writing the recipe
# --------------------------------------------------------------------------- #
def apply_req_diff(existing_run: list[str], diff: dict) -> list[str]:
    """Build the full new `requirements.run` list by applying `diff` onto the
    existing block.

    `diff` is `{conda_name: {"old", "new", ...}}` (only changed deps, from
    prepare's compute_req_diff). Existing entries whose name is in the diff get
    their range replaced with `new`; entries not in the diff (e.g. the `python`
    pin, unchanged deps) are kept verbatim; deps in the diff with no existing
    entry (new deps) are appended. Ordering of existing entries is preserved.
    """
    result: list[str] = []
    seen: set[str] = set()
    for entry in existing_run:
        name = _entry_name(entry)
        seen.add(name)
        if name in diff:
            new_spec = diff[name]["new"]
            result.append(f"{name} {new_spec}".strip() if new_spec else name)
        else:
            result.append(entry)
    # Append brand-new deps (in the diff but not already in the run block).
    for name, change in diff.items():
        if name not in seen:
            new_spec = change["new"]
            result.append(f"{name} {new_spec}".strip() if new_spec else name)
    return result


def apply_update(recipe_text: str, version: str, sha256: str, run: list[str]) -> str:
    """Return updated recipe text: version, sha256, build number reset, and the
    run block replaced with `run` (preserving indentation)."""
    text = re.sub(
        r'(context:\s*\n(?:.*\n)*?\s+version:\s*)["\'][^"\']+["\']',
        f'\\g<1>"{version}"',
        text := recipe_text,
    )
    text = re.sub(r"(sha256:\s*)[0-9a-f]{64}", f"\\g<1>{sha256}", text)
    text = re.sub(r"(  number:\s*)\d+", r"\g<1>0", text)
    new_block = "\n".join(f"    - {e}" for e in run) + "\n"
    text = re.sub(r"(\n  run:\n)(?:    - .*\n)+", f"\\g<1>{new_block}", text)
    return text
