#!/usr/bin/env bash
# Fetch the released sdist's PyPI metadata as one compact JSON line.
# Inputs (env): PYPI_NAME, VERSION
set -euo pipefail

curl -fsSL "https://pypi.org/pypi/${PYPI_NAME}/${VERSION}/json" | jq -c .
