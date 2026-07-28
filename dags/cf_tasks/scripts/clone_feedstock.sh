#!/usr/bin/env bash
# Clone conda-forge/<package>-feedstock into the local feedstocks dir (idempotent).
# Inputs (env): PACKAGE, FEEDSTOCKS_ROOT
set -euo pipefail

mkdir -p "$FEEDSTOCKS_ROOT"
dir="$FEEDSTOCKS_ROOT/${PACKAGE}-feedstock"

if [ -d "$dir/.git" ]; then
  echo "already cloned: $dir"
  git -C "$dir" fetch origin --quiet
else
  gh repo clone "conda-forge/${PACKAGE}-feedstock" "$dir"
fi
echo "$dir"
