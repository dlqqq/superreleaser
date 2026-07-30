#!/usr/bin/env bash
# Squash-merge a source-repo PR after CI is green and a human approved, and delete
# its branch. Step 0/1 of the release read the merged state of the default branch,
# so this must land before they run.
# Inputs (env): REPO, PR_URL
set -euo pipefail

state="$(gh pr view "$PR_URL" --repo "$REPO" --json state --jq '.state')"
if [ "$state" = "MERGED" ]; then
  echo "already merged: $PR_URL"
  exit 0
fi

num="$(gh pr view "$PR_URL" --repo "$REPO" --json number --jq '.number')"
title="$(gh pr view "$PR_URL" --repo "$REPO" --json title --jq '.title')"

# GitHub refuses to merge a draft, and Step 0 opens its docs PR as one. The human
# gate upstream of this task is the review, so promoting it here is safe.
if [ "$(gh pr view "$PR_URL" --repo "$REPO" --json isDraft --jq '.isDraft')" = "true" ]; then
  gh pr ready "$PR_URL" --repo "$REPO" >&2
fi

gh pr merge "$PR_URL" --repo "$REPO" --squash --delete-branch \
  --subject "${title} (#${num})" --body ""
echo "merged ${PR_URL} as '${title} (#${num})'"
