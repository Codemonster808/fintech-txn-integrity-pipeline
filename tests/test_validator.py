"""Runs against a live MiniStack instance (see conftest-less setup in README/BUILD_GUIDE)."""
import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common import aws  # noqa: E402
from validator import handler  # noqa: E402


def _event(**overrides) -> dict:
    base = {
        "txn_id": str(uuid.uuid4()),
        "idempotency_key": str(uuid.uuid4()),
        "account_id": "acc_test",
        "amount_cents": 100,
        "currency": "USD",
        "schema_version": 1,
        "ts": "2026-01-01T00:00:00Z",
    }
    base.update(overrides)
    return base


def test_valid_event_lands_in_raw_bucket():
    event = _event()
    result = handler(event)
    assert result["valid"] is True
    assert result["bucket"] == "txn-raw"

    s3 = aws.client("s3")
    obj = s3.get_object(Bucket="txn-raw", Key=result["key"])
    assert json.loads(obj["Body"].read())["txn_id"] == event["txn_id"]


def test_schema_mismatch_is_quarantined_not_dropped():
    event = _event(schema_version=99)
    result = handler(event)
    assert result["valid"] is False
    assert result["bucket"] == "txn-quarantine"
    assert "schema_version mismatch" in result["reason"]

    s3 = aws.client("s3")
    obj = s3.get_object(Bucket="txn-quarantine", Key=result["key"])
    body = json.loads(obj["Body"].read())
    assert body["_quarantine_reason"]


def test_missing_required_field_is_quarantined():
    event = _event()
    del event["amount_cents"]
    result = handler(event)
    assert result["valid"] is False
    assert "missing fields" in result["reason"]


def test_negative_amount_is_quarantined():
    event = _event(amount_cents=-500)
    result = handler(event)
    assert result["valid"] is False
    assert "positive integer" in result["reason"]
