"""conda-forge lookups + PyPI metadata.

The hard-won lesson (verified against anaconda.org): there is NO deterministic
hyphen/underscore rule for conda-forge names. conda-forge uses `jupyter_server`
(underscore) but `jupyterlab-chat` and `agent-client-protocol` (hyphen). So a
name MUST be probed against the API, never guessed. Anything unresolvable is
flagged for a human — the DAG opens a DRAFT PR and comments, rather than
inventing a name.
"""

from __future__ import annotations

import functools
import json
import re
import urllib.error
import urllib.request

_ANACONDA = "https://api.anaconda.org/package/conda-forge/{name}"
_PYPI = "https://pypi.org/pypi/{name}/{version}/json"


def _get_json(url: str) -> dict | None:
    """GET JSON; None on 404, raise on other HTTP errors."""
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


@functools.lru_cache(maxsize=256)
def cf_package(name: str) -> dict | None:
    """Return the conda-forge package record (incl. `versions`) or None."""
    return _get_json(_ANACONDA.format(name=name))


def resolve_conda_name(pypi_name: str) -> str | None:
    """Probe conda-forge for the package matching a PyPI distribution name.

    Tries the name as-is, then the hyphen and underscore variants. Returns the
    conda-forge name that actually exists, or None if none do (→ the caller
    flags it for a human). Never fabricates a name.
    """
    base = pypi_name.strip().lower()
    seen: list[str] = []
    for cand in (base, base.replace("_", "-"), base.replace("-", "_")):
        if cand in seen:
            continue
        seen.append(cand)
        if cf_package(cand) is not None:
            return cand
    return None


def is_published(conda_name: str, version: str) -> bool:
    """Is this exact version present on the conda-forge channel (anaconda.org)?

    Bypasses the lru_cache (fetches fresh) so it's safe to call in a poll loop.
    The anaconda.org package DB reflects an upload within minutes; the CDN
    repodata that `conda install` reads lags (~30 min historically) — this
    checks the DB, the earliest reliable "it shipped" signal. Only meaningful
    once the post-merge build is green: a green build means the package was
    uploaded, so absence here is propagation lag, not a missing release."""
    version = version.lstrip("v")
    data = _get_json(_ANACONDA.format(name=conda_name))
    return bool(data) and version in (data.get("versions") or [])


def version_available(conda_name: str, spec: str) -> bool:
    """Does at least one build on conda-forge satisfy `spec` (a PEP 440 range
    like '>=2.4.0,<3')? Empty spec means "any version present" → True if the
    package exists at all."""
    from packaging.specifiers import InvalidSpecifier, SpecifierSet
    from packaging.version import InvalidVersion, Version

    rec = cf_package(conda_name)
    if rec is None:
        return False
    versions = rec.get("versions", [])
    if not spec:
        return bool(versions)
    try:
        ss = SpecifierSet(spec)
    except InvalidSpecifier:
        # Unparseable spec → don't block; presence is the best signal we have.
        return bool(versions)
    for v in versions:
        try:
            if Version(v) in ss:
                return True
        except InvalidVersion:
            continue
    return False


# --------------------------------------------------------------------------- #
# PyPI
# --------------------------------------------------------------------------- #
def pypi_release(pypi_name: str, version: str) -> dict | None:
    return _get_json(_PYPI.format(name=pypi_name, version=version.lstrip("v")))


def sdist_sha256(pypi_name: str, version: str) -> str | None:
    data = pypi_release(pypi_name, version)
    if not data:
        return None
    for u in data["urls"]:
        if u["url"].endswith(".tar.gz"):
            return u["digests"]["sha256"]
    return None


def runtime_requirements(pypi_name: str, version: str) -> list[tuple[str, str]]:
    """Parse the released sdist's `requires_dist` into [(dep_name, spec), ...],
    dropping extras (test/dev) and environment-marker-only deps. This is the
    package's declared runtime deps — the source of truth for run-ranges, NOT an
    AI guess."""
    data = pypi_release(pypi_name, version)
    if not data:
        return []
    out: list[tuple[str, str]] = []
    for req in data["info"].get("requires_dist") or []:
        if "extra ==" in req:
            continue
        head = req.split(";")[0].strip()
        m = re.match(r"^([A-Za-z0-9_.\-]+)\s*(.*)$", head)
        if not m:
            continue
        dep = m.group(1)
        spec = m.group(2).strip().strip("()").strip()
        out.append((dep, spec))
    return out
