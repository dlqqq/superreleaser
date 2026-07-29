#!/usr/bin/env bash
# Cleanup: delete the draft GitHub release if it exists AND is still a draft.
# A published release is no longer a draft (isDraft=false), so this only ever
# removes an abandoned draft (rejected gate or a failed run) — never a real
# release. RELEASE_URL is the draft URL from prep_release; its tag is the last
# path segment (e.g. untagged-<hash>). Inputs (env): REPO, RELEASE_URL
set -euo pipefail

if [ -z "${RELEASE_URL:-}" ]; then
  echo "no draft release URL recorded (nothing to delete)"
  exit 0
fi

tag="${RELEASE_URL##*/releases/tag/}"
is_draft="$(gh release view "$tag" --repo "$REPO" --json isDraft \
             --jq '.isDraft' 2>/dev/null || echo "missing")"

case "$is_draft" in
  true)    gh release delete "$tag" --repo "$REPO" --yes --cleanup-tag
           echo "deleted abandoned draft release $tag" ;;
  false)   echo "release $tag was published (not a draft) — leaving it" ;;
  *)       echo "no release $tag found (nothing to delete)" ;;
esac
