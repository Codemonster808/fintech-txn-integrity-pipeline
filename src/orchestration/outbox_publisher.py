#!/usr/bin/env python3
"""Publishes PENDING rows from txn-outbox to SNS, then marks them PUBLISHED.

    source env.sh
    python3 src/orchestration/outbox_publisher.py

This is a separate process from src/orchestration/lambdas/record_status.py on purpose:
record_status.py's job is to commit the business fact and the pending
event atomically (one DynamoDB transaction, see its docstring) — it must
NOT also try to publish to SNS in the same breath, because that would
reintroduce exactly the failure mode the outbox exists to avoid (a
publish that fails after the business write already committed).

This publisher is idempotent and safe to re-run: if it crashes between
publishing an event and marking it PUBLISHED, the next run publishes it
again (at-least-once delivery — a duplicate CurationCompleted event is a
much smaller problem than a silently lost one). Run it whenever; nothing
breaks if it hasn't run in a while — PENDING rows just accumulate in
txn-outbox until it does.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from utils import aws  # noqa: E402

OUTBOX_TABLE = "txn-outbox"
TOPIC_NAME = "txn-curation-events"


def _pending_events(ddb) -> list[dict]:
    resp = ddb.scan(
        TableName=OUTBOX_TABLE,
        FilterExpression="#s = :pending",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":pending": {"S": "PENDING"}},
    )
    return resp.get("Items", [])


def publish_pending() -> dict:
    ddb = aws.client("dynamodb")
    sns = aws.client("sns")
    topic_arn = sns.create_topic(Name=TOPIC_NAME)["TopicArn"]  # idempotent lookup-by-name

    pending = _pending_events(ddb)
    published = 0
    for item in pending:
        event_id = item["event_id"]["S"]
        payload = item["payload"]["S"]
        sns.publish(TopicArn=topic_arn, Message=payload, Subject=item["event_type"]["S"])
        ddb.update_item(
            TableName=OUTBOX_TABLE,
            Key={"event_id": {"S": event_id}},
            UpdateExpression="SET #s = :published",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":published": {"S": "PUBLISHED"}},
        )
        print(f"  published: {event_id}")
        published += 1

    if not pending:
        print("  no PENDING events")
    return {"published": published}


def main() -> None:
    print("Outbox publisher — txn-outbox -> txn-curation-events:")
    result = publish_pending()
    print(f"done: {result['published']} event(s) published")


if __name__ == "__main__":
    main()
