"""
Exercises the Go idempotency gate against a live MiniStack instance.
Requires: `docker compose up -d`, `python3 scripts/bootstrap.py`, and the
gate binary built (`cd src/ingestion/gate && go build ./...`) and running on :8080
(see Makefile `check-ingest` target, which drives this same flow).
"""

import os
import subprocess
import time
import uuid
from pathlib import Path

import pytest
import requests

GATE_URL = "http://localhost:8080"
GATE_BIN = Path(__file__).resolve().parents[2] / "src" / "ingestion" / "gate" / "gate"


@pytest.fixture(scope="module")
def gate_process():
    if not GATE_BIN.exists():
        pytest.skip(
            f"gate binary not built — run `cd src/ingestion/gate && go build ./...` first ({GATE_BIN})"
        )
    # Inherit the full environment (not just GIN_MODE) so AWS_ENDPOINT_URL —
    # this machine runs MiniStack on 4581, per CLAUDE.md §5 — actually reaches
    # the gate subprocess. Replacing the env entirely here would silently fall
    # back to the Go binary's hardcoded localhost:4566 default and talk to
    # whatever unrelated MiniStack happens to be listening there.
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


def _event(idempotency_key: str, txn_id: str) -> dict:
    return {
        "txn_id": txn_id,
        "idempotency_key": idempotency_key,
        "account_id": "acc_test",
        "amount_cents": 100,
        "currency": "USD",
        "schema_version": 1,
        "ts": "2026-01-01T00:00:00Z",
    }


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
