#!/usr/bin/env bash
# Open the feedstock PR: base = conda-forge/<pkg>-feedstock default branch,
# head = <fork-owner>:release-<version>. Everything is explicit (repo/base/head)
# so it doesn't depend on `gh repo set-default` or which remote is "default".
# Title is exactly "<pkg> v<version>" so a squash merge yields "… (#N)".
# Prints the PR URL as the last line (for XCom). Inputs (env): WORKTREE, PACKAGE, VERSION
set -euo pipefail

cd "$WORKTREE"
base_repo="conda-forge/${PACKAGE}-feedstock"
owner="$(gh api user --jq .login)"          # where `gh repo fork` put the fork
head="${owner}:release-${VERSION}"
base="$(git symbolic-ref --short refs/remotes/origin/HEAD | sed 's|origin/||')"
title="${PACKAGE} v${VERSION}"

# Reuse an already-open PR for this head (safe on re-runs), else create one.
url="$(gh pr list --repo "$base_repo" --head "release-${VERSION}" \
        --state open --json url --jq '.[0].url // ""')"
if [ -z "$url" ]; then
  gh pr create \
    --repo "$base_repo" \
    --base "$base" \
    --head "$head" \
    --title "$title" \
    --body "Update ${PACKAGE} to ${VERSION}." >&2
  url="$(gh pr list --repo "$base_repo" --head "release-${VERSION}" \
          --state open --json url --jq '.[0].url')"
fi

echo "$url"
