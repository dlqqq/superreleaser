root := justfile_directory()

# A single AIRFLOW_HOME under the repo (SQLite db, logs, generated password).
export AIRFLOW_HOME := root / ".airflow"
export AIRFLOW__CORE__DAGS_FOLDER := root / "dags"
# Localhost-only: disable auth so there's no password prompt/login.
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS := "True"

alias list := list-recipes

# List all available recipes
list-recipes:
    @just --list --list-heading=""

# Install dependencies into a local uv-managed venv (.venv).
setup:
    uv sync --extra dev
    @echo "✓ Installed. Run 'just start' to launch Airflow."

# Start Airflow all-in-one (API server + scheduler + triggerer) on SQLite.
# Foreground/long-lived — leave it running, then open http://localhost:8080.
start:
    @echo "Airflow UI → http://localhost:8080 (auth disabled; localhost only)"
    uv run airflow standalone

# Run the unit tests (no network; covers the recipe/version/plan logic).
test:
    uv run pytest -q

# Trigger the single-package release DAG (dry-run by default).
# Force a target with version=0.2.0; set dry_run=false for a real release.
release package version="" dry_run="true":
    #!/usr/bin/env bash
    set -eo pipefail
    conf=$(printf '{"package":"%s","version":"%s","dry_run":%s}' "{{ package }}" "{{ version }}" "{{ dry_run }}")
    echo "conf: $conf"
    uv run airflow dags trigger cf_release --conf "$conf"

# Trigger the batch DAG from a plan JSON file (default: the sample two-wave plan).
release-batch plan=(root / "sample_batch_plan.json"):
    #!/usr/bin/env bash
    set -eo pipefail
    conf=$(cat "{{ plan }}")
    echo "plan ({{ plan }}):"; echo "$conf"
    uv run airflow dags trigger cf_release_batch --conf "$conf"
