"""Shared bootstrap + constants for the cf_release task-group modules."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# The `superreleaser` package lives at the repo root, one level above dags/.
# Ensure it's importable regardless of where Airflow loads DAGs from.
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from superreleaser import config  # noqa: E402

log = logging.getLogger("superreleaser.dag")

# Folder holding the step scripts; the DAG adds this to its template_searchpath
# so BashOperator(bash_command="<name>.sh") resolves.
SCRIPTS = str(Path(__file__).resolve().parent / "scripts")

# Common env passed to the bash step scripts. append_env=True MERGES onto the
# inherited environment (keeps SSH_AUTH_SOCK, PATH, HOME) rather than replacing.
# RUN_KEY is the DAG run_id sanitized for use in a filesystem path — every task
# that touches the /tmp worktree builds it from RUN_KEY so they all agree and
# runs never collide (unique per run, unlike a package+version path).
ENV = {
    "PACKAGE": "{{ params.package }}",
    "VERSION": "{{ params.version }}",
    "FEEDSTOCKS_ROOT": str(config.FEEDSTOCKS_ROOT),
    "RUN_KEY": "{{ run_id | replace(':','-') | replace('+','-') | replace('.','-') }}",
}
BASE = dict(append_env=True)


def conf(context, key, default=None):
    """Read a key from the triggering `dag_run.conf` (falling back to default)."""
    return (context["dag_run"].conf or {}).get(key, default)
