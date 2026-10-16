"""
Shared test setup for the capture-agent test suite.

* Puts the agent root on `sys.path` so `import src...` works from any cwd.
* `src.config.Config` reads `DATABASE_URL` from the environment at import
  time, so a placeholder is provided before anything imports it. Unit tests
  never connect to it; integration tests use `TEST_DATABASE_URL` instead
  (see tests/integration/conftest.py).
"""

import os
import sys
from pathlib import Path

AGENT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(AGENT_ROOT))

os.environ.setdefault("DATABASE_URL", "postgresql://unit-tests.invalid/none")
