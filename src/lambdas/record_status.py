"""
Lambda-shaped handler: writes a curation-job status row to DynamoDB.
Called twice per daily run — once by the pre-flight state machine
execution (status=started) and once by the post-curation execution
(status=completed, with row counts) — see src/statemachine.py, the
Python driver that runs the actual PySpark curate.py step in between
(Step Functions doesn't run Spark inside a Lambda; it orchestrates
around a compute job the same way it would trigger and poll an EMR
step or Glue job in real AWS).
"""
import time

import boto3

JOBS_TABLE = "txn-curation-jobs"


def handler(event, context):
    endpoint = "http://127.0.0.1:4566"
    ddb = boto3.client("dynamodb", endpoint_url=endpoint, region_name="us-east-1")

    job_id = event["job_id"]
    status = event["status"]

    # job_id alone is the table's hash key, but a job has 2+ status rows
    # (started, completed) — key on "{job_id}#{status}" so the second
    # put_item doesn't overwrite the first.
    item = {
        "job_id": {"S": f"{job_id}#{status}"},
        "status": {"S": status},
        "recorded_at": {"N": str(int(time.time()))},
    }
    for key in ("raw_object_count", "curated_row_count", "duplicates_dropped"):
        if key in event:
            item[key] = {"N": str(event[key])}

    ddb.put_item(TableName=JOBS_TABLE, Item=item)
    return {"recorded": True, "job_id": job_id, "status": status}
