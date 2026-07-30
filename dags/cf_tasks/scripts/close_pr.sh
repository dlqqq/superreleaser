#!/usr/bin/env bash
# Close the release PR if one is still open (best-effort). Resolves the PR by its
# head branch rather than a URL, so it works even when the run failed before the
# open-PR step produced a URL. A merged PR is not "open", so this never touches a
# successful release. Inputs (env): FEEDSTOCK_REPO, VERSION
set -euo pipefail

url="$(gh pr list --repo "$FEEDSTOCK_REPO" --head "release-${VERSION}" \
        --state open --json url --jq '.[0].url // ""')"

if [ -n "$url" ]; then
  gh pr close "$url" --repo "$FEEDSTOCK_REPO" --delete-branch
  echo "closed open PR: $url"
else
  echo "no open PR for release-${VERSION} (nothing to close)"
fi
