#!/usr/bin/env bash
# Squash-merge the feedstock PR after CI is green and a human approved. Pass the
# subject explicitly so the squash commit is exactly "<pkg> v<version> (#N)"
# regardless of the repo's squash-title default. Operates via --repo, so it
# needs no local checkout. Inputs (env): FEEDSTOCK_REPO, PACKAGE, VERSION, PR_URL
set -euo pipefail

num=$(gh pr view "$PR_URL" --repo "$FEEDSTOCK_REPO" --json number --jq '.number')
gh pr merge "$PR_URL" --repo "$FEEDSTOCK_REPO" --squash \
  --subject "${PACKAGE} v${VERSION} (#${num})" \
  --body ""
echo "merged ${PR_URL} as '${PACKAGE} v${VERSION} (#${num})'"
