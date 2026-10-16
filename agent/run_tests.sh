#!/usr/bin/env bash
# Runs all tests in ./tests.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

VENV_PYTHON="../.venv/bin/python"
if [ -x "$VENV_PYTHON" ]; then
    PYTHON="$VENV_PYTHON"
else
    PYTHON="python3"
fi

exec "$PYTHON" -m pytest tests "$@"
