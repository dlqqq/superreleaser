#!/usr/bin/env bash
# Delete the fork's remote release branch if it exists (best-effort). Covers the
# case where the branch was pushed but no PR was opened (or the PR was closed
# without --delete-branch), so a re-run's push starts clean.
# Inputs (env): FEEDSTOCK_NAME, VERSION
set -euo pipefail

owner="$(gh api user --jq .login)"           # the fork lives under this user
fork="${owner}/${FEEDSTOCK_NAME}"
branch="release-${VERSION}"

if gh api "repos/${fork}/branches/${branch}" >/dev/null 2>&1; then
  gh api -X DELETE "repos/${fork}/git/refs/heads/${branch}"
  echo "deleted fork branch ${fork}:${branch}"
else
  echo "no fork branch ${fork}:${branch} (nothing to delete)"
fi
