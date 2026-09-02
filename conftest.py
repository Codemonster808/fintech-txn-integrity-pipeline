"""Repo-root pytest fixtures, shared by `tests/` and `features/steps/`.

`make test` runs `pytest tests/ features/`, and both trees need the same live
Go gate. Defining `gate_process` here (rather than in one test module that the
others import) is what lets pytest inject it by name into either tree: a
fixture imported into a module and then re-declared as a test parameter is a
redefinition (ruff F811), while a conftest fixture is simply found.
"""

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent / "tests"))

from gate_helpers import GATE_BIN, GATE_URL  # noqa: E402


@pytest.fixture(scope="module")
def gate_process():
    if not GATE_BIN.exists():
        pytest.skip(
            "gate binary not built — run `cd src/ingestion/gate && go build ./...` "
            f"first ({GATE_BIN})"
        )
    # Inherit the full environment (not just GIN_MODE) so AWS_ENDPOINT_URL —
    # this machine runs MiniStack on 4581, per CLAUDE.md §5 — actually reaches
    # the gate subprocess. Replacing the env entirely here would silently fall
    # back to the Go binary's own default and talk to whatever unrelated
    # MiniStack happens to be listening there.
    proc = subprocess.Popen([str(GATE_BIN)], env={**os.environ, "GIN_MODE": "release"})
    for _ in range(20):
        try:
            if requests.get(f"{GATE_URL}/health", timeout=1).status_code == 200:
                break
        except requests.ConnectionError:
            time.sleep(0.25)
    else:
        proc.terminate()
        pytest.fail("gate did not become healthy in time")
    yield proc
    proc.terminate()
    proc.wait(timeout=5)
