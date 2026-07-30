"""Shared bootstrap + the per-release identity that parameterizes every group.

The task groups (pypi, conda_forge, and its phases) take an `ident` argument
rather than reading `params.package` — that's what lets them be used BOTH by the
single-package DAGs and inside a *mapped* task group (`simple_jai_release`),
where each expansion releases a different package.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from airflow.sdk import task

# The `superreleaser` package lives at the repo root, one level above dags/.
# Ensure it's importable regardless of where Airflow loads DAGs from.
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from superreleaser import config, registry  # noqa: E402

log = logging.getLogger("superreleaser.dag")

# Folder holding the step scripts; the DAG adds this to its template_searchpath
# so BashOperator(bash_command="<name>.sh") resolves.
SCRIPTS = str(Path(__file__).resolve().parent / "scripts")

# Exposed to Jinja via the DAG's user_defined_macros so a template can resolve
# registry fields, e.g. "{{ pkg(params.package).cf_pkg_name }}". Register with:
#   @dag(..., user_defined_macros=MACROS)
MACROS = {"pkg": registry.get}

# append_env=True MERGES onto the inherited environment (keeps SSH_AUTH_SOCK,
# PATH, HOME) rather than replacing it — git-over-SSH and conda need those.
BASE = dict(append_env=True)

# The env keys `identity` produces; also the keys of the ENV dict passed to the
# bash step scripts.
IDENT_KEYS = (
    "PACKAGE", "VERSION", "PYPI_NAME", "CF_PKG_NAME", "REPO",
    "FEEDSTOCK_REPO", "FEEDSTOCK_NAME", "FEEDSTOCKS_ROOT", "RUN_KEY",
)


@task(
    task_id="identity",
    task_display_name="Resolve package identity",
    multiple_outputs=True,
)
def identity(package: str, version: str, **context) -> dict:
    """Resolve everything the release steps need to know about one package.

    This is the linchpin of the whole design. Registry names (feedstock repo,
    conda-forge name, PyPI name) are NOT derivable from the package name, so they
    must be looked up — and doing that lookup in a *task* rather than in Jinja is
    what makes the groups reusable: a mapped expansion resolves its own package,
    while the single-package DAGs resolve `params.package`. Same code either way.

    Returns a dict whose keys are exactly the env vars the bash step scripts
    read; `env_from(ident)` turns it into a BashOperator `env=`.

    `multiple_outputs=True` pushes each key as its own XCom, which is what makes
    `ident["PACKAGE"]` resolvable — a keyed XComArg pulls that key, and without it
    only `return_value` exists.
    """
    p = registry.get(package)
    version = version.strip().lstrip("v")
    if not version:
        raise ValueError(f"no version given for {package!r}")
    return {
        "PACKAGE": package,
        "VERSION": version,
        "PYPI_NAME": p.pypi_name,
        "CF_PKG_NAME": p.cf_pkg_name,
        "REPO": p.repo,
        "FEEDSTOCK_REPO": p.feedstock_repo,
        "FEEDSTOCK_NAME": p.feedstock_repo.split("/")[1],
        "FEEDSTOCKS_ROOT": str(config.FEEDSTOCKS_ROOT),
        # The DAG run_id sanitized for a filesystem path. Every task that
        # touches the /tmp worktree builds its path from RUN_KEY so they all
        # agree, and runs never collide.
        "RUN_KEY": _run_key(context["run_id"]),
    }


def _run_key(run_id: str) -> str:
    """Sanitize a run_id into a filesystem-safe path segment."""
    for ch in (":", "+", "."):
        run_id = run_id.replace(ch, "-")
    return run_id


def env_from(ident, **extra) -> dict:
    """Build a BashOperator `env=` from an `identity` XComArg (plus extras).

    Each value is `ident["<KEY>"]` — an XComArg key lookup, which Airflow
    resolves per task instance when it renders `env` (a template field). In a
    mapped group that resolution is per map index, so every expansion gets its
    own package's values.
    """
    return {k: ident[k] for k in IDENT_KEYS} | extra


def sibling_task_id(task_id: str, sibling: str) -> str:
    """The id of a task sitting in the same group as `task_id`.

    `"pypi_release.delete_rejected_draft"` + `"prep_release"` →
    `"pypi_release.prep_release"`. Deriving sibling ids instead of hardcoding
    them keeps the groups nesting-agnostic: the same code works at the DAG top
    level and inside `simple_jai_release`'s mapped group, where every id carries
    an extra prefix.
    """
    prefix = task_id.rsplit(".", 1)[0] + "." if "." in task_id else ""
    return f"{prefix}{sibling}"


def conf(context, key, default=None):
    """Read a key from the triggering `dag_run.conf` (falling back to default)."""
    return (context["dag_run"].conf or {}).get(key, default)
