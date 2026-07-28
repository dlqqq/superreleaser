root := justfile_directory()

# A single AIRFLOW_HOME under the repo (SQLite db, logs, generated password).
export AIRFLOW_HOME := root / ".airflow"
export AIRFLOW__CORE__DAGS_FOLDER := root / "dags"
# Localhost-only: disable auth so there's no password prompt/login.
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS := "True"
# Don't load Airflow's bundled example DAGs (keeps the UI to just our DAGs).
# Must be set before the DB is first initialized; `just reset` clears a DB that
# already loaded them.
export AIRFLOW__CORE__LOAD_EXAMPLES := "False"

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

# Reset the local Airflow home (SQLite db, logs, config). Use this if example
# DAGs were loaded before LOAD_EXAMPLES=False took effect. Recreated on next start.
reset:
    rm -rf "$AIRFLOW_HOME"
    @echo "✓ Removed $AIRFLOW_HOME — next 'just start' reinitializes it clean."

# Run the unit tests (no network; covers the recipe/version/plan logic).
test:
    uv run pytest -q

# Trigger the conda-forge release DAG for a package + explicit version. It opens
# a real feedstock PR and merges after CI green + your approval in the UI — so
# `just start` the server first and drive the gate there.
release package version:
    #!/usr/bin/env bash
    set -eo pipefail
    conf=$(printf '{"package":"%s","version":"%s"}' "{{ package }}" "{{ version }}")
    echo "conf: $conf"
    uv run airflow dags trigger cf_release --conf "$conf"
