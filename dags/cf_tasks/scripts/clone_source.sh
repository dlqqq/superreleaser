#!/usr/bin/env bash
# Clone a *source* repo (not a feedstock) locally and check out a fresh branch off
# the default branch, so a task can edit tracked files and open a PR. Idempotent:
# fetches an existing clone and recreates the branch from scratch.
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

default=$(git -C "$clone" symbolic-ref --short refs/remotes/origin/HEAD | sed 's|origin/||')

# Start from a clean default branch, then (re)create the work branch. Detach
# first so `branch -f` can move BRANCH even if it's currently checked out.
git -C "$clone" checkout --detach --quiet "origin/${default}"
git -C "$clone" reset --hard --quiet "origin/${default}"
git -C "$clone" clean -fdq
git -C "$clone" checkout -B "$BRANCH" --quiet "origin/${default}"

echo "$clone"
