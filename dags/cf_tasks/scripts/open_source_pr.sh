#!/usr/bin/env bash
# Commit the working-tree edits in a source-repo checkout, push the branch to the
# `fork` remote (set up by clone_source.sh), and open a PR against the upstream
# repo's default branch. We never push a branch to the upstream repo itself, even
# where we have write access — same rule the feedstock flow follows.
# The PR is labelled `maintenance` (best-effort) so the changelog builds.
# Prints the PR URL as the last line (for XCom).
# Inputs (env): REPO, CHECKOUT, BRANCH, TITLE, BODY
set -euo pipefail

cd "$CHECKOUT"

if git diff --quiet && git diff --cached --quiet; then
  echo "ERROR: no changes to commit in $CHECKOUT" >&2
  exit 1
fi

git add -A
git commit -m "$TITLE" >&2
git push --force-with-lease -u fork "$BRANCH" >&2

base="$(git symbolic-ref --short refs/remotes/origin/HEAD | sed 's|origin/||')"
owner="$(gh api user --jq .login)"          # where `gh repo fork` put the fork
head="${owner}:${BRANCH}"

# Reuse an already-open PR for this head (safe on re-runs), else create one.
url="$(gh pr list --repo "$REPO" --head "$BRANCH" --state open \
        --json url --jq '.[0].url // ""')"
if [ -z "$url" ]; then
  gh pr create --repo "$REPO" --base "$base" --head "$head" \
    --title "$TITLE" --body "$BODY" --label maintenance >&2 \
    || gh pr create --repo "$REPO" --base "$base" --head "$head" \
         --title "$TITLE" --body "$BODY" >&2
  url="$(gh pr list --repo "$REPO" --head "$BRANCH" --state open \
          --json url --jq '.[0].url')"
fi

echo "$url"
