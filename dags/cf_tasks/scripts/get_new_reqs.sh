#!/usr/bin/env bash
# Fetch the released sdist's PyPI metadata as one compact JSON line.
# Inputs (env): PACKAGE, VERSION
set -euo pipefail

curl -fsSL "https://pypi.org/pypi/${PACKAGE}/${VERSION}/json" | jq -c .
