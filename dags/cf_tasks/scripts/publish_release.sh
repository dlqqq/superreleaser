#!/usr/bin/env bash
# Run "Step 2: Publish Release" on the source repo with the draft release URL
# from Step 1, and watch it to completion. This is what actually publishes to
# PyPI. Inputs (env): REPO, RELEASE_URL, SOURCE_BRANCH (optional)
set -euo pipefail

# Pass SOURCE_BRANCH as jupyter-releaser's `branch` input when set (must match
# the branch used in Step 1); blank = repo default.
args=(-f release_url="$RELEASE_URL")
if [ -n "${SOURCE_BRANCH:-}" ]; then
  args+=(-f branch="$SOURCE_BRANCH")
fi
gh workflow run "Step 2: Publish Release" --repo "$REPO" "${args[@]}" >&2

sleep 5
run_id="$(gh run list --repo "$REPO" --workflow "Step 2: Publish Release" \
          --limit 1 --json databaseId --jq '.[0].databaseId')"
gh run watch "$run_id" --repo "$REPO" --exit-status >&2
echo "published via run $run_id"
