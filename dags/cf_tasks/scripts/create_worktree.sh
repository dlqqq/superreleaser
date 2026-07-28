#!/usr/bin/env bash
# Create a fresh git worktree under /tmp for the release branch, off the local
# feedstock clone's default branch. Idempotent: removes a stale worktree/branch
# for this version first. Prints the worktree path as the last line (for XCom).
# Inputs (env): PACKAGE, VERSION, FEEDSTOCKS_ROOT
set -euo pipefail

clone="$FEEDSTOCKS_ROOT/${PACKAGE}-feedstock"
worktree="/tmp/${PACKAGE}-feedstock-${VERSION}"
branch="release-${VERSION}"

git -C "$clone" fetch origin --quiet
default=$(git -C "$clone" symbolic-ref --short refs/remotes/origin/HEAD | sed 's|origin/||')

# Clean up any previous attempt for this version.
git -C "$clone" worktree remove --force "$worktree" 2>/dev/null || true
rm -rf "$worktree"
git -C "$clone" branch -D "$branch" 2>/dev/null || true

git -C "$clone" worktree add -b "$branch" "$worktree" "origin/${default}" >&2
echo "$worktree"
