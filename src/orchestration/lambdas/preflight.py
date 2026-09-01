"""
Lambda-shaped handler: counts valid raw objects and rejects the run
early if there's nothing to curate. Deployed to MiniStack Lambda and
invoked as a Step Functions Task with Retry/Catch (see asl/preflight.json
and asl/postflight.json).
"""

import os

import boto3


def handler(event, context):
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
    s3 = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1")

    resp = s3.list_objects_v2(Bucket="txn-raw", Prefix="valid/")
    count = resp.get("KeyCount", 0)

    return {"raw_object_count": count, "ready_to_curate": count > 0}
