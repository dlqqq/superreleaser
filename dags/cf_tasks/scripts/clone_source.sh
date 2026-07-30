#!/usr/bin/env bash
# Clone a *source* repo (not a feedstock) locally, ensure a `fork` remote exists,
# and check out a fresh branch off the default branch, so a task can edit tracked
# files and open a PR from the fork. Idempotent: fetches an existing clone and
# recreates the branch from scratch.
# Prints the checkout path as the last line (for XCom).
# Inputs (env): REPO, SOURCES_ROOT, BRANCH
set -euo pipefail

name="${REPO##*/}"
clone="${SOURCES_ROOT}/${name}"

if [ -d "$clone/.git" ]; then
  git -C "$clone" fetch origin --quiet
else
  mkdir -p "$SOURCES_ROOT"
  gh repo clone "$REPO" "$clone" >&2
fi

# PRs come from a personal fork, never a branch on the upstream repo — same rule
# the feedstock flow follows (see ensure_fork.sh).
if ! git -C "$clone" remote get-url fork >/dev/null 2>&1; then
  echo "Forking ${REPO}..." >&2
  (cd "$clone" && gh repo fork --remote --remote-name fork) >&2
fi

# Prune so `fork/$BRANCH` reflects reality: the push below uses
# --force-with-lease, which rejects a create when a stale tracking ref names a
# branch that no longer exists on the fork (e.g. deleted after a merge).
git -C "$clone" fetch fork --prune --quiet

default=$(git -C "$clone" symbolic-ref --short refs/remotes/origin/HEAD | sed 's|origin/||')

# Start from a clean default branch, then (re)create the work branch. Detach
# first so `branch -f` can move BRANCH even if it's currently checked out.
git -C "$clone" checkout --detach --quiet "origin/${default}"
git -C "$clone" reset --hard --quiet "origin/${default}"
git -C "$clone" clean -fdq
git -C "$clone" checkout -B "$BRANCH" --quiet "origin/${default}"

echo "$clone"
