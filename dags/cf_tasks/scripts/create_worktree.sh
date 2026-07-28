#!/usr/bin/env bash
# Create a fresh git worktree under /tmp for the release branch, off the local
# feedstock clone's default branch. Idempotent: removes a stale worktree/branch
# for this version first. Prints the worktree path as the last line (for XCom).
# Inputs (env): PACKAGE, VERSION, FEEDSTOCKS_ROOT
#
# The worktree leaf dir MUST be exactly "<pkg>-feedstock": conda-smithy rerender
# derives the feedstock repo name from the directory basename, so a dir like
# "<pkg>-feedstock-<version>" would rewrite README links to a bogus
# "<pkg>-feedstock-<version>-feedstock" repo. So nest under a run-scoped parent:
# /tmp/superreleaser-<run-key>/<pkg>-feedstock
# Inputs (env) also include RUN_KEY (the sanitized DAG run_id).
set -euo pipefail

clone="$FEEDSTOCKS_ROOT/${PACKAGE}-feedstock"
parent="/tmp/superreleaser-${RUN_KEY}"
worktree="${parent}/${PACKAGE}-feedstock"
branch="release-${VERSION}"

git -C "$clone" fetch origin --quiet
default=$(git -C "$clone" symbolic-ref --short refs/remotes/origin/HEAD | sed 's|origin/||')

# Clean up any previous attempt for this version. `worktree prune` clears stale
# registrations from earlier runs (whose /tmp dirs may be gone) so the branch is
# no longer considered "checked out" and `branch -D` can actually delete it.
git -C "$clone" worktree remove --force "$worktree" 2>/dev/null || true
rm -rf "$parent"
git -C "$clone" worktree prune 2>/dev/null || true
git -C "$clone" branch -D "$branch" 2>/dev/null || true

mkdir -p "$parent"
git -C "$clone" worktree add -b "$branch" "$worktree" "origin/${default}" >&2
echo "$worktree"
