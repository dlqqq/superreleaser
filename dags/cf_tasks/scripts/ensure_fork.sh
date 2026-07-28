#!/usr/bin/env bash
# Ensure a `fork` remote exists on the feedstock clone (conda-forge PRs come
# from a fork). Inputs (env): PACKAGE, FEEDSTOCKS_ROOT
set -euo pipefail

cd "$FEEDSTOCKS_ROOT/${PACKAGE}-feedstock"

if git remote get-url fork >/dev/null 2>&1; then
  echo "fork remote already set"
else
  echo "Forking ${PACKAGE}-feedstock..."
  gh repo fork --remote --remote-name fork
  gh repo set-default "$(git remote get-url origin | sed 's|.*github.com[:/]||;s|\.git$||')"
fi
