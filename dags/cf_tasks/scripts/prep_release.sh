#!/usr/bin/env bash
# Run "Step 1: Prep Release" on the source repo and watch it to completion, then
# emit the draft-release URL as the last line (for XCom). Jupyter Releaser's
# prep-release action prints `Setting output release_url=...` in the run log;
# that's the stable place to read the draft URL from.
# Inputs (env): REPO, VERSION, SOURCE_BRANCH (optional)
set -euo pipefail

# Kick off Step 1 (version_spec = the explicit version we're releasing). When a
# SOURCE_BRANCH is given, pass it as jupyter-releaser's `branch` input so the
# release is cut from that branch (e.g. a 0.2.x backport); blank = repo default.
# The workflow itself is still dispatched on the repo's default ref, so the
# workflow file is always the one on the default branch.
#
# since_last_stable=true always: build the changelog from PRs with activity
# since the last *stable* git tag (jupyter-releaser's `since_last_stable` input,
# a boolean that defaults to false/unchecked). This keeps prereleases from
# truncating the changelog to only-since-the-last-prerelease.
args=(-f version_spec="$VERSION" -f since_last_stable=true)
if [ -n "${SOURCE_BRANCH:-}" ]; then
  args+=(-f branch="$SOURCE_BRANCH")
fi
gh workflow run "Step 1: Prep Release" --repo "$REPO" "${args[@]}" >&2

# The run doesn't appear instantly; wait for the newest prep-release run to show
# up, then watch it to completion (fail the task if it fails).
sleep 5
run_id="$(gh run list --repo "$REPO" --workflow "Step 1: Prep Release" \
          --limit 1 --json databaseId --jq '.[0].databaseId')"
gh run watch "$run_id" --repo "$REPO" --exit-status >&2

# Extract the draft release URL the prep action set as an output.
url="$(gh run view "$run_id" --repo "$REPO" --log 2>/dev/null \
        | grep -oE 'release_url=https://[^ ]+/releases/tag/[^ ]+' \
        | head -1 | sed 's/release_url=//')"
if [ -z "$url" ]; then
  echo "ERROR: could not find draft release URL in run $run_id" >&2
  exit 1
fi
echo "$url"
