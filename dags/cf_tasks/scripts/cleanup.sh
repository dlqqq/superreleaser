#!/usr/bin/env bash
# Remove the /tmp release worktree AND delete the local release branch, so a
# re-run's `git worktree add -b release-<version>` doesn't collide with a
# leftover branch. Runs at the end of a release regardless of outcome.
# Inputs (env): FEEDSTOCK_NAME, VERSION, FEEDSTOCKS_ROOT, RUN_KEY
set -euo pipefail

# Per-PACKAGE parent (must match create_worktree.sh): N packages release in
# parallel within one run, so removing a run-scoped parent would destroy
# siblings' worktrees.
clone="$FEEDSTOCKS_ROOT/${FEEDSTOCK_NAME}"
parent="/tmp/superreleaser-${RUN_KEY}/${FEEDSTOCK_NAME}"
worktree="${parent}/${FEEDSTOCK_NAME}"
branch="release-${VERSION}"

# 1. Remove the worktree and drop its registration (prune clears any stale
#    entries too, so the branch is no longer "checked out" and can be deleted).
git -C "$clone" worktree remove --force "$worktree" 2>/dev/null || true
rm -rf "$parent"
git -C "$clone" worktree prune 2>/dev/null || true

# 2. Delete the local release branch (best-effort; may already be gone).
git -C "$clone" branch -D "$branch" 2>/dev/null || true

echo "removed worktree ${worktree} and local branch ${branch}"
