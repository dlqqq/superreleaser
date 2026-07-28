"""Shared bootstrap + helpers for the cf_release task modules."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# The `superreleaser` package lives at the repo root, one level above dags/.
# Ensure it's importable regardless of where Airflow loads DAGs from.
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

log = logging.getLogger("superreleaser.dag")


def conf(context, key, default=None):
    """Read a key from the triggering `dag_run.conf` (falling back to default)."""
    return (context["dag_run"].conf or {}).get(key, default)
