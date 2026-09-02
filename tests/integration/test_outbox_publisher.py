"""PENDING → PUBLISHED is the transactional-outbox contract.

record_status.py writes PENDING atomically with the job row; this
publisher is the separate process that actually talks to SNS. Spec:
docs/specs/spec-transactional-outbox.md, ADR 0002.
Requires MiniStack + bootstrap (txn-outbox table).
"""

import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from orchestration.outbox_publisher import (  # noqa: E402
    OUTBOX_TABLE,
    _pending_events,
    publish_pending,
)
from utils import aws  # noqa: E402


def _put_pending(event_id: str, payload: dict) -> None:
    ddb = aws.client("dynamodb")
    ddb.put_item(
        TableName=OUTBOX_TABLE,
        Item={
            "event_id": {"S": event_id},
            "event_type": {"S": "CurationCompleted"},
            "payload": {"S": json.dumps(payload)},
            "status": {"S": "PENDING"},
        },
    )


def test_publish_pending_flips_status_to_published():
    event_id = f"pytest-outbox-{uuid.uuid4()}#CurationCompleted"
    _put_pending(event_id, {"job_id": "pytest", "curated_row_count": 1})

    result = publish_pending()
    assert result["published"] >= 1

    ddb = aws.client("dynamodb")
    item = ddb.get_item(TableName=OUTBOX_TABLE, Key={"event_id": {"S": event_id}})["Item"]
    assert item["status"]["S"] == "PUBLISHED"

    still_pending_ids = {e["event_id"]["S"] for e in _pending_events(ddb)}
    assert event_id not in still_pending_ids


def test_second_publish_is_a_no_op_for_already_published_rows():
    event_id = f"pytest-outbox-{uuid.uuid4()}#CurationCompleted"
    _put_pending(event_id, {"job_id": "pytest-idem", "curated_row_count": 0})
    publish_pending()
    publish_pending()

    ddb = aws.client("dynamodb")
    item = ddb.get_item(TableName=OUTBOX_TABLE, Key={"event_id": {"S": event_id}})["Item"]
    assert item["status"]["S"] == "PUBLISHED"
    pending_ids = {e["event_id"]["S"] for e in _pending_events(ddb)}
    assert event_id not in pending_ids
