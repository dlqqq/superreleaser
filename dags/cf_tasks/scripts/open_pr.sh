#!/usr/bin/env bash
# Open the feedstock PR from the fork's release branch. Title is exactly
# "<pkg> v<version>" so a squash merge produces "<pkg> v<version> (#N)".
# Prints the PR URL as the last line (for XCom).
# Inputs (env): WORKTREE, PACKAGE, VERSION
set -euo pipefail

cd "$WORKTREE"
title="${PACKAGE} v${VERSION}"
gh pr create \
  --title "$title" \
  --body "Update ${PACKAGE} to ${VERSION}." \
  --head "release-${VERSION}" >&2

# Emit the URL for this branch's PR as the sole last line.
gh pr view "release-${VERSION}" --json url --jq '.url'
