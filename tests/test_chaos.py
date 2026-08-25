"""
Failure-mode tests — the part of the repo that's supposed to be hard.
Requires the gate binary built and MiniStack running (same as test_idempotency.py).
"""
import concurrent.futures
import time
import uuid
from pathlib import Path

import pytest
import requests

from test_idempotency import GATE_BIN, GATE_URL, _event, gate_process  # noqa: F401,E402


def test_concurrent_duplicate_requests_only_one_wins(gate_process):
    """
    A retry storm: N identical requests fired at once. Exactly one must be
    accepted — this is the case a naive "check-then-write" (as opposed to
    an atomic conditional PutItem) would fail under real concurrency.
    """
    key = f"idem-chaos-race-{uuid.uuid4()}"
    n_requests = 20

    def send(i: int):
        return requests.post(f"{GATE_URL}/accept", json=_event(key, f"txn-race-{i}"))

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_requests) as pool:
        responses = list(pool.map(send, range(n_requests)))

    status_codes = [r.status_code for r in responses]
    assert status_codes.count(200) == 1, f"expected exactly 1 acceptance, got {status_codes.count(200)}"
    assert status_codes.count(409) == n_requests - 1


def test_burst_of_distinct_keys_all_succeed(gate_process):
    """A burst of legitimately distinct transactions must not throttle each other out."""
    n_requests = 30
    run_id = uuid.uuid4()

    def send(i: int):
        return requests.post(f"{GATE_URL}/accept", json=_event(f"idem-burst-{run_id}-{i}", f"txn-burst-{i}"))

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_requests) as pool:
        responses = list(pool.map(send, range(n_requests)))

    assert all(r.status_code == 200 for r in responses)


def test_late_retry_after_delay_is_still_rejected(gate_process):
    """A retry arriving well after the original (not a tight race) must still be caught."""
    key = f"idem-chaos-late-{uuid.uuid4()}"
    first = requests.post(f"{GATE_URL}/accept", json=_event(key, "txn-original"))
    assert first.status_code == 200

    time.sleep(1.0)  # simulate a delayed retry, not a tight race

    late_retry = requests.post(f"{GATE_URL}/accept", json=_event(key, "txn-late-retry"))
    assert late_retry.status_code == 409
