#!/usr/bin/env bash
# Remove the /tmp release worktree (and deregister it from the clone). Runs at
# the end of a release regardless of outcome. Inputs (env): PACKAGE, VERSION,
# FEEDSTOCKS_ROOT
set -euo pipefail

clone="$FEEDSTOCKS_ROOT/${PACKAGE}-feedstock"
worktree="/tmp/${PACKAGE}-feedstock-${VERSION}"

git -C "$clone" worktree remove --force "$worktree" 2>/dev/null || true
rm -rf "$worktree"
echo "removed worktree: $worktree"
