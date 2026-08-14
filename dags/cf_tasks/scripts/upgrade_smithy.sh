#!/usr/bin/env bash
# Upgrade conda-smithy to the newest version allowed by pixi.toml BEFORE the
# rerender step runs. conda-smithy self-guards on staleness and aborts the
# rerender when the installed version is older than the one a feedstock pins:
#   RuntimeError: conda-smithy version (2026.6.14) is out-of-date (2026.8.9) ...
# pixi.toml floors conda-smithy with no ceiling (conda-smithy >= ...), so a
# `pixi update` re-solves it to the latest release and installs it into the very
# same env this DAG's tasks run from — so the subsequent `conda smithy rerender`
# subprocess picks up the fresh binary.
set -euo pipefail

cd "${PIXI_PROJECT_ROOT:?PIXI_PROJECT_ROOT is not set (run under pixi)}"

# Refresh just conda-smithy (and whatever it needs) to the newest allowed by the
# floor-only constraint; leave the rest of the lock untouched.
pixi update conda-smithy >&2

# Surface the resolved version in the task log for auditability.
conda smithy --version >&2
