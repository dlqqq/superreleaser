#!/usr/bin/env bash
# Verify every dependency in the diff exists on the conda-forge channel using
# `conda search` (exit 0 = found, exit 1 = missing). Fails listing any misses
# so a human resolves the name before a PR is opened.
# Inputs (env): DIFF (JSON object keyed by conda-forge name)
set -euo pipefail

missing=""
for name in $(echo "$DIFF" | jq -r 'keys[]'); do
  if conda search -c conda-forge "$name" >/dev/null 2>&1; then
    echo "${name}: found"
  else
    echo "${name}: MISSING"
    missing="${missing} ${name}"
  fi
done

if [ -n "$missing" ]; then
  echo "ERROR: no conda-forge package for:${missing}" >&2
  exit 1
fi
echo "all dependencies exist on conda-forge"
