"""
Lambda-shaped handler: writes a curation-job status row to DynamoDB.
Called twice per daily run — once by the pre-flight state machine
execution (status=started) and once by the post-curation execution
(status=completed, with row counts) — see src/orchestration/statemachine.py, the
Python driver that runs the actual PySpark curate.py step in between
(Step Functions doesn't run Spark inside a Lambda; it orchestrates
around a compute job the same way it would trigger and poll an EMR
step or Glue job in real AWS).

On status=completed, this also writes a PENDING row to txn-outbox in the
SAME transact_write_items call as the job-status row — the transactional
outbox pattern: the business fact ("this job completed") and the
not-yet-published event are committed atomically, or neither is. If this
handler instead called sns.publish() directly after the put_item, a
publish failure right after a successful write would silently lose the
event while the business fact stayed recorded. src/orchestration/outbox_publisher.py
is the separate process that actually publishes txn-outbox rows to SNS
and marks them PUBLISHED — see its docstring for why that's a distinct
step, not inlined here.
"""

import json
import time

import boto3

JOBS_TABLE = "txn-curation-jobs"
OUTBOX_TABLE = "txn-outbox"


def handler(event, context):
    endpoint = "http://127.0.0.1:4566"
    ddb = boto3.client("dynamodb", endpoint_url=endpoint, region_name="us-east-1")

    job_id = event["job_id"]
    status = event["status"]
    recorded_at = int(time.time())

    # job_id alone is the table's hash key, but a job has 2+ status rows
    # (started, completed) — key on "{job_id}#{status}" so the second
    # put_item doesn't overwrite the first.
    item = {
        "job_id": {"S": f"{job_id}#{status}"},
        "status": {"S": status},
        "recorded_at": {"N": str(recorded_at)},
    }
    for key in ("raw_object_count", "curated_row_count", "duplicates_dropped"):
        if key in event:
            item[key] = {"N": str(event[key])}

    if status != "completed":
        ddb.put_item(TableName=JOBS_TABLE, Item=item)
        return {"recorded": True, "job_id": job_id, "status": status}

    payload = {
        "job_id": job_id,
        "curated_row_count": event.get("curated_row_count"),
        "duplicates_dropped": event.get("duplicates_dropped"),
    }
    outbox_item = {
        "event_id": {"S": f"{job_id}#CurationCompleted"},
        "event_type": {"S": "CurationCompleted"},
        "payload": {"S": json.dumps(payload)},
        "status": {"S": "PENDING"},
        "created_at": {"N": str(recorded_at)},
    }
    ddb.transact_write_items(
        TransactItems=[
            {"Put": {"TableName": JOBS_TABLE, "Item": item}},
            {"Put": {"TableName": OUTBOX_TABLE, "Item": outbox_item}},
        ]
    )
    return {
        "recorded": True,
        "job_id": job_id,
        "status": status,
        "outbox_event": "CurationCompleted",
    }
