#!/usr/bin/env python3
"""
Schema validator: Lambda-shaped (handler(event, context) entry point,
deployable as-is to MiniStack/AWS Lambda) but invoked directly here to
avoid packaging overhead that adds no signal to a portfolio repo.

Reads accepted events, checks schema_version against the registry in S3,
writes valid events to txn-raw and invalid ones to txn-quarantine instead
of dropping them silently.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils import aws  # noqa: E402

SCHEMA_REGISTRY_BUCKET = "txn-raw"
SCHEMA_REGISTRY_KEY = "_schema_registry/current_version.json"
RAW_BUCKET = "txn-raw"
QUARANTINE_BUCKET = "txn-quarantine"


def ensure_schema_registry(s3) -> None:
    try:
        s3.head_object(Bucket=SCHEMA_REGISTRY_BUCKET, Key=SCHEMA_REGISTRY_KEY)
    except s3.exceptions.ClientError:
        s3.put_object(
            Bucket=SCHEMA_REGISTRY_BUCKET,
            Key=SCHEMA_REGISTRY_KEY,
            Body=json.dumps({"current_version": 1}).encode(),
        )


def current_schema_version(s3) -> int:
    obj = s3.get_object(Bucket=SCHEMA_REGISTRY_BUCKET, Key=SCHEMA_REGISTRY_KEY)
    return json.loads(obj["Body"].read())["current_version"]


REQUIRED_FIELDS = {
    "txn_id",
    "idempotency_key",
    "account_id",
    "amount_cents",
    "currency",
    "schema_version",
    "ts",
}


def validate_event(event: dict, expected_version: int) -> tuple[bool, str]:
    missing = REQUIRED_FIELDS - event.keys()
    if missing:
        return False, f"missing fields: {sorted(missing)}"
    if event.get("schema_version") != expected_version:
        return (
            False,
            f"schema_version mismatch: got {event.get('schema_version')}, expected {expected_version}",
        )
    if not isinstance(event.get("amount_cents"), int) or event["amount_cents"] <= 0:
        return False, "amount_cents must be a positive integer"
    return True, ""


def handler(event: dict, context=None) -> dict:
    """Lambda-shaped entry point. `event` here is a single txn event dict."""
    s3 = aws.client("s3")
    ensure_schema_registry(s3)
    expected_version = current_schema_version(s3)

    ok, reason = validate_event(event, expected_version)
    bucket = RAW_BUCKET if ok else QUARANTINE_BUCKET
    key = f"{'valid' if ok else 'invalid'}/{event.get('idempotency_key', 'unknown')}.json"

    payload = dict(event)
    if not ok:
        payload["_quarantine_reason"] = reason

    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload).encode())
    return {"valid": ok, "bucket": bucket, "key": key, "reason": reason}


def main() -> None:
    """CLI batch mode: validate every event in a JSONL file."""
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="in_path", required=True)
    args = parser.parse_args()

    n_valid = n_invalid = 0
    with open(args.in_path) as f:
        for line in f:
            event = json.loads(line)
            result = handler(event)
            if result["valid"]:
                n_valid += 1
            else:
                n_invalid += 1

    print(f"validated {n_valid + n_invalid} events: {n_valid} valid, {n_invalid} quarantined")


if __name__ == "__main__":
    main()
