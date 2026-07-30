#!/usr/bin/env bash
# Push the finished release branch to the fork. Runs after the branch is fully
# built (recipe + rerender committed), so CI runs once on the final head.
# Inputs (env): WORKTREE, VERSION
set -euo pipefail

cd "$WORKTREE"
git push -u fork "release-${VERSION}"
