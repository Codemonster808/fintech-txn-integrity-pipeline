"""
Lambda-shaped handler: counts valid raw objects and rejects the run
early if there's nothing to curate. Deployed to MiniStack Lambda and
invoked as a Step Functions Task with Retry/Catch (see asl/preflight.json
and asl/postflight.json).
"""

import boto3


def handler(event, context):
    # Hardcoded to the container-internal address on purpose, matching every
    # other Lambda in this portfolio (record_status.py here, check_budget.py in
    # agentic-claims-copilot, etc.): this code runs INSIDE MiniStack's own
    # container, where the port is always 4566 regardless of the host-side
    # mapping (4581 in this repo). Reading AWS_ENDPOINT_URL here would inherit
    # the HOST address and be unreachable from in here.
    endpoint = "http://127.0.0.1:4566"
    s3 = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1")

    resp = s3.list_objects_v2(Bucket="txn-raw", Prefix="valid/")
    count = resp.get("KeyCount", 0)

    return {"raw_object_count": count, "ready_to_curate": count > 0}
