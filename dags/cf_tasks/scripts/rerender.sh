#!/usr/bin/env bash
# Re-render the feedstock locally with conda-smithy, leaving the changes in the
# worktree UNCOMMITTED (no `--commit`) — the single `commit.sh` step captures the
# recipe bump and the rerender output together. Inputs (env): WORKTREE
set -euo pipefail

cd "$WORKTREE"
conda smithy rerender
