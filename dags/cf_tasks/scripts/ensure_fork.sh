#!/usr/bin/env bash
# Ensure a `fork` remote exists on the feedstock clone (conda-forge PRs come
# from a fork). Inputs (env): FEEDSTOCK_NAME, FEEDSTOCKS_ROOT
set -euo pipefail

cd "$FEEDSTOCKS_ROOT/${FEEDSTOCK_NAME}"

if git remote get-url fork >/dev/null 2>&1; then
  echo "fork remote already set"
else
  echo "Forking ${FEEDSTOCK_NAME}..."
  # Creates the fork under the authenticated user and adds the `fork` remote.
  # (No `gh repo set-default` — every gh command below is fully explicit about
  # its repo/base/head, so it doesn't depend on per-clone default state.)
  gh repo fork --remote --remote-name fork
fi
