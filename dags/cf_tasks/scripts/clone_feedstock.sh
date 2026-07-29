#!/usr/bin/env bash
# Clone the feedstock into the local feedstocks dir (idempotent).
# Inputs (env): FEEDSTOCK_REPO, FEEDSTOCK_NAME, FEEDSTOCKS_ROOT
set -euo pipefail

mkdir -p "$FEEDSTOCKS_ROOT"
dir="$FEEDSTOCKS_ROOT/${FEEDSTOCK_NAME}"

if [ -d "$dir/.git" ]; then
  echo "already cloned: $dir"
  git -C "$dir" fetch origin --quiet
else
  gh repo clone "$FEEDSTOCK_REPO" "$dir"
fi
echo "$dir"
