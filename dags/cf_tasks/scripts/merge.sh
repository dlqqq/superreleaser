#!/usr/bin/env bash
# Squash-merge the feedstock PR after CI is green and a human approved. Pass the
# subject explicitly so the squash commit is exactly "<pkg> v<version> (#N)"
# regardless of the repo's squash-title default. Operates via --repo, so it
# needs no local checkout. Inputs (env): PACKAGE, VERSION, PR_URL
set -euo pipefail

repo="conda-forge/${PACKAGE}-feedstock"
num=$(gh pr view "$PR_URL" --repo "$repo" --json number --jq '.number')
gh pr merge "$PR_URL" --repo "$repo" --squash \
  --subject "${PACKAGE} v${VERSION} (#${num})" \
  --body ""
echo "merged ${PR_URL} as '${PACKAGE} v${VERSION} (#${num})'"
