"""BDD steps for idempotency.feature.

Uses the gate_process fixture (repo-root conftest.py) and the _event()/GATE_URL
helpers from tests/gate_helpers.py (the same ones test_chaos.py uses)
against the real Go gate + MiniStack DynamoDB — no new behavior invented
here, just the already-passing test flows wrapped as Given/When/Then.
"""

import concurrent.futures
import sys
import time
import uuid
from pathlib import Path

import requests
from pytest_bdd import given, parsers, scenarios, then, when

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tests"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from gate_helpers import GATE_URL, _event  # noqa: E402

from utils import aws  # noqa: E402

# `gate_process` is injected by the repo-root conftest.py, not imported.

scenarios("../idempotency.feature")


def _idempotency_table():
    return aws.resource("dynamodb").Table("txn-idempotency")


def _row_count(idempotency_key: str) -> int:
    resp = _idempotency_table().get_item(Key={"idempotency_key": idempotency_key})
    return 1 if "Item" in resp else 0


@given("a first transaction accepted through the gate", target_fixture="ctx")
def first_accepted(gate_process):
    key = f"idem-bdd-{uuid.uuid4()}"
    resp = requests.post(f"{GATE_URL}/accept", json=_event(key, "txn-bdd-original"))
    assert resp.status_code == 200
    return {"key": key}


@given("a brand-new idempotency_key", target_fixture="ctx")
def brand_new_key(gate_process):
    return {"key": f"idem-bdd-concurrent-{uuid.uuid4()}"}


@when("the same idempotency_key is resent")
def resend_same_key(ctx):
    ctx["retry_response"] = requests.post(
        f"{GATE_URL}/accept", json=_event(ctx["key"], "txn-bdd-retry")
    )


@when("a retry with the same key arrives 1 second later")
def resend_after_delay(ctx):
    time.sleep(1.0)
    ctx["retry_response"] = requests.post(
        f"{GATE_URL}/accept", json=_event(ctx["key"], "txn-bdd-late-retry")
    )


@when(parsers.parse("{n:d} concurrent requests are sent with that key"))
def send_concurrent(ctx, n):
    key = ctx["key"]

    def send(i: int):
        return requests.post(f"{GATE_URL}/accept", json=_event(key, f"txn-bdd-concurrent-{i}"))

    with concurrent.futures.ThreadPoolExecutor(max_workers=n) as pool:
        ctx["responses"] = list(pool.map(send, range(n)))


@then("the retry is rejected with 409")
def retry_rejected(ctx):
    resp = ctx["retry_response"]
    assert resp.status_code == 409
    assert resp.json()["status"] == "duplicate"


@then("no new row is created in txn-idempotency")
def no_new_row(ctx):
    assert _row_count(ctx["key"]) == 1


@then(parsers.parse("exactly {n:d} request is accepted with 200"))
def accepted_count(ctx, n):
    codes = [r.status_code for r in ctx["responses"]]
    assert codes.count(200) == n


@then(parsers.parse("exactly {n:d} requests are rejected with 409"))
def rejected_count(ctx, n):
    codes = [r.status_code for r in ctx["responses"]]
    assert codes.count(409) == n
