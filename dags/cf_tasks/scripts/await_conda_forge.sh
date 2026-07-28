#!/usr/bin/env bash
# One availability probe: does <package> == <version> exist on conda-forge?
# Exit 0 = available (sensor completes), exit 1 = not yet (sensor keeps polling).
# The DAG polls this every ~60s. Inputs (env): PACKAGE, VERSION
set -euo pipefail

if conda search -c conda-forge "${PACKAGE}==${VERSION}" >/dev/null 2>&1; then
  echo "${PACKAGE}==${VERSION} is available on conda-forge"
  exit 0
fi
echo "${PACKAGE}==${VERSION} not on conda-forge yet"
exit 1
