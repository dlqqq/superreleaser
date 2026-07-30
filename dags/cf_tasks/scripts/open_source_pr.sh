#!/usr/bin/env bash
# Commit the working-tree edits in a source-repo checkout, push the branch, and
# open a PR against the repo's default branch. Unlike the feedstock flow, this
# pushes to a branch on the repo itself (we have write access to jupyter-ai) —
# no fork, so the PR is mergeable by the DAG.
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
git push --force-with-lease -u origin "$BRANCH" >&2

base="$(git symbolic-ref --short refs/remotes/origin/HEAD | sed 's|origin/||')"

# Reuse an already-open PR for this head (safe on re-runs), else create one.
url="$(gh pr list --repo "$REPO" --head "$BRANCH" --state open \
        --json url --jq '.[0].url // ""')"
if [ -z "$url" ]; then
  gh pr create --repo "$REPO" --base "$base" --head "$BRANCH" \
    --title "$TITLE" --body "$BODY" >&2
  url="$(gh pr list --repo "$REPO" --head "$BRANCH" --state open \
          --json url --jq '.[0].url')"
fi

echo "$url"
