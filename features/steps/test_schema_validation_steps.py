"""BDD steps for schema-validation.feature — pure validate_event, no S3."""
import sys
import uuid
from pathlib import Path

from pytest_bdd import given, parsers, scenarios, then, when

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from models.validator import validate_event  # noqa: E402

scenarios("../schema-validation.feature")


def _base(**overrides):
    event = {
        "txn_id": str(uuid.uuid4()),
        "idempotency_key": str(uuid.uuid4()),
        "account_id": "acc_test",
        "amount_cents": 100,
        "currency": "USD",
        "schema_version": 1,
        "ts": "2026-01-01T00:00:00Z",
    }
    event.update(overrides)
    return event


@given(parsers.parse("a transaction event with schema_version {version:d}"), target_fixture="event")
def event_wrong_version(version):
    return _base(schema_version=version)


@given(parsers.parse("a transaction event with amount_cents {amount:d}"), target_fixture="event")
def event_amount(amount):
    return _base(amount_cents=amount)


@given("a transaction event missing amount_cents", target_fixture="event")
def event_missing_amount():
    event = _base()
    del event["amount_cents"]
    return event


@given("a complete valid transaction event", target_fixture="event")
def event_valid():
    return _base()


@when("the event is checked against registry version 1", target_fixture="result")
def check_event(event):
    return validate_event(event, 1)


@then("the event is rejected")
def rejected(result):
    ok, _reason = result
    assert ok is False


@then("the event is accepted")
def accepted(result):
    ok, _reason = result
    assert ok is True


@then(parsers.parse("the reason mentions {snippet}"))
def reason_mentions(result, snippet):
    _ok, reason = result
    assert snippet in reason
