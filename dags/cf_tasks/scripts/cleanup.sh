#!/usr/bin/env bash
# Remove the /tmp release worktree (and deregister it from the clone). Runs at
# the end of a release regardless of outcome. Inputs (env): PACKAGE,
# FEEDSTOCKS_ROOT, RUN_KEY
set -euo pipefail

clone="$FEEDSTOCKS_ROOT/${PACKAGE}-feedstock"
parent="/tmp/superreleaser-${RUN_KEY}"
worktree="${parent}/${PACKAGE}-feedstock"

git -C "$clone" worktree remove --force "$worktree" 2>/dev/null || true
rm -rf "$parent"
echo "removed worktree: $worktree"
