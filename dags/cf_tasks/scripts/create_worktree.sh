#!/usr/bin/env bash
# Create a fresh git worktree under /tmp for the release branch, off the local
# feedstock clone's default branch. Idempotent: removes a stale worktree/branch
# for this version first. Prints the worktree path as the last line (for XCom).
# Inputs (env): FEEDSTOCK_NAME, VERSION, FEEDSTOCKS_ROOT, RUN_KEY,
#               FEEDSTOCK_BRANCH (optional)
#
# The worktree leaf dir MUST be exactly the feedstock repo name: conda-smithy
# rerender derives the feedstock repo name from the directory basename, so a dir
# like "<feedstock>-<version>" would rewrite README links to a bogus
# "<feedstock>-<version>-feedstock" repo. So nest under a run-scoped parent:
# /tmp/superreleaser-<run-key>/<feedstock-name>
set -euo pipefail

clone="$FEEDSTOCKS_ROOT/${FEEDSTOCK_NAME}"
parent="/tmp/superreleaser-${RUN_KEY}"
worktree="${parent}/${FEEDSTOCK_NAME}"
branch="release-${VERSION}"

git -C "$clone" fetch origin --quiet
# Base branch: the explicit FEEDSTOCK_BRANCH (e.g. a 0.2.x backport branch) when
# given, otherwise the feedstock's actual default branch via origin/HEAD
# (main/master — asked, not assumed).
if [ -n "${FEEDSTOCK_BRANCH:-}" ]; then
  base="$FEEDSTOCK_BRANCH"
else
  base=$(git -C "$clone" symbolic-ref --short refs/remotes/origin/HEAD | sed 's|origin/||')
fi

# Clean up any previous attempt for this version. `worktree prune` clears stale
# registrations from earlier runs (whose /tmp dirs may be gone) so the branch is
# no longer considered "checked out" and `branch -D` can actually delete it.
git -C "$clone" worktree remove --force "$worktree" 2>/dev/null || true
rm -rf "$parent"
git -C "$clone" worktree prune 2>/dev/null || true
git -C "$clone" branch -D "$branch" 2>/dev/null || true

mkdir -p "$parent"
git -C "$clone" worktree add -b "$branch" "$worktree" "origin/${base}" >&2
echo "$worktree"
