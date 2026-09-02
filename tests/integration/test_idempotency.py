"""
Exercises the Go idempotency gate against a live MiniStack instance.
Requires: `docker compose up -d`, `python3 scripts/bootstrap.py`, and the
gate binary built (`cd src/ingestion/gate && go build ./...`) and running on :8080
(see Makefile `check-ingest` target, which drives this same flow).
"""

import sys
import uuid
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gate_helpers import GATE_URL, _event  # noqa: E402

# The `gate_process` fixture lives in the repo-root conftest.py so both this
# suite and features/steps/ get it injected by name — see that file for why.


def test_first_send_is_accepted(gate_process):
    key = f"idem-pytest-{uuid.uuid4()}"
    resp = requests.post(f"{GATE_URL}/accept", json=_event(key, "txn-a"))
    assert resp.status_code == 200
    assert resp.json()["status"] == "accepted"


def test_duplicate_idempotency_key_is_rejected(gate_process):
    key = f"idem-pytest-{uuid.uuid4()}"
    first = requests.post(f"{GATE_URL}/accept", json=_event(key, "txn-b"))
    assert first.status_code == 200

    retry = requests.post(f"{GATE_URL}/accept", json=_event(key, "txn-c"))
    assert retry.status_code == 409
    assert retry.json()["status"] == "duplicate"


def test_different_keys_are_independent(gate_process):
    a = requests.post(f"{GATE_URL}/accept", json=_event(f"idem-pytest-{uuid.uuid4()}", "txn-d"))
    b = requests.post(f"{GATE_URL}/accept", json=_event(f"idem-pytest-{uuid.uuid4()}", "txn-e"))
    assert a.status_code == 200
    assert b.status_code == 200
