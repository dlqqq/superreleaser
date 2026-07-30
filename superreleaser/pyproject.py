"""Editing jupyter-ai's dependency ranges in `pyproject.toml`.

`simple_jai_release` releases a wave of subpackages, then cuts a `jupyter-ai`
release whose dependency ranges point at the versions just published. That range
edit happens here, as surgical text substitution (never a TOML round-trip): a
round-trip would reorder keys, drop comments, and reformat unrelated tables in a
file humans maintain.

Two range modes per subpackage:

- `"auto"` (the default) — raise the FLOOR to the released version, keep the
  existing ceiling. If the new floor would meet or exceed that ceiling, the bump
  is a lie (the range would exclude the very version being released), so it
  raises `RangeError` and fails the run rather than shipping a broken range.
- an explicit spec (e.g. `">=0.3.0,<0.4.0"`) — used verbatim.

Both `[project.dependencies]` and every `[project.optional-dependencies]` extra
(`magics`, `jupyternaut`, …) are covered. Only packages named in the release
input are touched; every other requirement is left exactly as it was.
"""

from __future__ import annotations

import re

from packaging.version import InvalidVersion, Version

AUTO = "auto"


class RangeError(Exception):
    """A range could not be bumped safely (e.g. the new floor breaks a ceiling)."""


def dist_key(name: str) -> str:
    """Normalize a distribution name for comparison (PEP 503-ish).

    jupyter-ai's pyproject writes `jupyter_ai_router` while the release input and
    the registry say `jupyter-ai-router`; both name the same distribution.
    """
    return re.sub(r"[-_.]+", "-", name.strip().lower())


def _split_clauses(spec: str) -> list[str]:
    return [c.strip() for c in spec.split(",") if c.strip()]


def bump_floor(existing_spec: str, version: str) -> str:
    """Raise the floor of `existing_spec` to `version`, keeping other clauses.

    The ceiling (and any other clause) is preserved verbatim and its position in
    the spec is kept, so the edit reads as a minimal diff.

    Raises RangeError if the released version isn't actually inside the resulting
    range — that means the existing ceiling excludes it (e.g. floor 0.4.0 against
    ceiling <0.4.0), which no floor bump can fix.
    """
    try:
        new = Version(version)
    except InvalidVersion as e:
        raise RangeError(f"invalid version {version!r}") from e

    clauses = _split_clauses(existing_spec)
    if not clauses:
        return f">={version}"

    out: list[str] = []
    replaced = False
    for clause in clauses:
        m = re.match(r"^(>=|>)\s*(.+)$", clause)
        if m and not replaced:
            out.append(f">={version}")
            replaced = True
            continue
        out.append(clause)
    if not replaced:  # no lower bound at all — prepend one
        out.insert(0, f">={version}")

    # Verify the released version satisfies every retained clause: an upper bound
    # that excludes it can't be fixed by bumping the floor, so fail loudly.
    for clause in out:
        m = re.match(r"^(<=|<)\s*(.+)$", clause)
        if not m:
            continue
        op, bound_text = m.group(1), m.group(2).strip()
        try:
            bound = Version(bound_text)
        except InvalidVersion:
            continue  # unparseable ceiling: leave it be, don't block the release
        ok = new <= bound if op == "<=" else new < bound
        if not ok:
            raise RangeError(
                f"new floor >={version} conflicts with existing ceiling "
                f"{op}{bound_text} — the range would exclude the version being "
                f"released. Set an explicit `range` for this subpackage."
            )
    return ",".join(out)


def new_spec(existing_spec: str, version: str, range_: str | None) -> str:
    """The replacement spec for one requirement: explicit range, or auto floor bump."""
    if range_ and range_.strip().lower() != AUTO:
        return range_.strip()
    return bump_floor(existing_spec, version)


# A requirement inside a pyproject dependency array, e.g.
#   "jupyter_ai_router>=0.0.5,<0.1.0",
# Captures: leading quote, distribution name, the version spec, closing quote.
_REQ = re.compile(
    r"""(?P<q>["'])(?P<name>[A-Za-z0-9._-]+)(?P<extras>\[[^\]]*\])?"""
    r"""(?P<spec>[^"']*)(?P=q)"""
)


def apply_ranges(text: str, wanted: dict[str, dict]) -> tuple[str, dict[str, str]]:
    """Rewrite the ranges of `wanted` requirements throughout a pyproject.

    `wanted` maps a normalized dist key (see `dist_key`) to
    `{"version": ..., "range": ...}`. Every requirement string in the file whose
    name matches is rewritten — covering `[project.dependencies]` and each
    `[project.optional-dependencies]` extra in one pass, including a package
    listed in several extras.

    Returns `(new_text, changes)`, where `changes` maps the dist key to a
    `"<old> → <new>"` summary for the PR body. Requirements not in `wanted` are
    untouched. Raises RangeError if any wanted range can't be bumped safely.
    """
    changes: dict[str, str] = {}
    errors: list[str] = []

    def repl(m: re.Match) -> str:
        key = dist_key(m.group("name"))
        want = wanted.get(key)
        if want is None:
            return m.group(0)
        old = m.group("spec").strip()
        try:
            spec = new_spec(old, want["version"], want.get("range"))
        except RangeError as e:
            errors.append(f"{m.group('name')}: {e}")
            return m.group(0)
        if spec != old:
            changes[key] = f"{old or '(none)'} → {spec}"
        q, extras = m.group("q"), m.group("extras") or ""
        return f"{q}{m.group('name')}{extras}{spec}{q}"

    new_text = _REQ.sub(repl, text)
    if errors:
        raise RangeError("; ".join(errors))
    return new_text, changes


def missing_from(text: str, wanted: dict[str, dict]) -> list[str]:
    """Wanted dist keys that appear nowhere in the pyproject's requirements.

    A subpackage named in the release input but absent from jupyter-ai's
    dependencies is almost certainly a typo or a stale registry entry, so the DAG
    surfaces it instead of silently bumping nothing.
    """
    found = {dist_key(m.group("name")) for m in _REQ.finditer(text)}
    return sorted(k for k in wanted if k not in found)
