#!/usr/bin/env bash
# Single commit capturing both the recipe bump and the rerender output, titled
# exactly "<pkg> v<version>" (so a squash merge yields "<pkg> v<version> (#N)").
# Stages everything the write + rerender changed. Inputs (env): WORKTREE,
# PACKAGE, VERSION
set -euo pipefail

cd "$WORKTREE"
git add -A
git commit -m "${PACKAGE} v${VERSION}"
