#!/usr/bin/env bash
# Run "Step 0: Prep release documentation" on the source repo, watch it, then find
# the docs PR it opened. Step 0 pushes a branch and opens a DRAFT PR; unlike
# Step 1 it doesn't emit the URL as a workflow output, so we look the PR up by
# head branch.
# Prints the docs PR URL as the last line (for XCom).
# Inputs (env): REPO, VERSION, TARGET_BRANCH
set -euo pipefail

# Step 0 normalizes the version to a v-prefix for the branch name, so match it:
# VERSION "3.2.0" → branch "release-docs/v3.2.0". Without this the PR lookup
# below finds nothing and the task fails after a successful workflow run.
case "$VERSION" in
  v*) v="$VERSION" ;;
  *)  v="v$VERSION" ;;
esac
branch="release-docs/${v}"

gh workflow run "Step 0: Prep release documentation" --repo "$REPO" \
  -f version="$VERSION" -f target-branch="$TARGET_BRANCH" >&2

# The run doesn't appear instantly; wait for it, then watch to completion.
sleep 5
run_id="$(gh run list --repo "$REPO" \
          --workflow "Step 0: Prep release documentation" \
          --limit 1 --json databaseId --jq '.[0].databaseId')"
gh run watch "$run_id" --repo "$REPO" --exit-status >&2

# The PR is opened by the workflow moments after the push; give it a few tries.
for _ in 1 2 3 4 5 6; do
  url="$(gh pr list --repo "$REPO" --head "$branch" --state open \
          --json url --jq '.[0].url // ""')"
  [ -n "$url" ] && break
  sleep 5
done

if [ -z "${url:-}" ]; then
  echo "ERROR: Step 0 finished but no open PR found for head ${branch}" >&2
  exit 1
fi
echo "$url"
