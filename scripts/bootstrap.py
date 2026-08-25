#!/usr/bin/env python3
"""Idempotent creation of the AWS resources this repo needs, against MiniStack."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common import aws  # noqa: E402

TOPIC_NAME = "txn-events"
VALIDATION_QUEUE = "txn-validation-queue"
AUDIT_QUEUE = "txn-audit-queue"
DLQ_NAME = "txn-audit-dlq"
IDEMPOTENCY_TABLE = "txn-idempotency"
METRICS_TABLE = "txn-gate-metrics"
JOBS_TABLE = "txn-curation-jobs"
BUCKETS = ["txn-raw", "txn-curated", "txn-quarantine"]


def ensure_bucket(s3, name: str) -> None:
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if name not in existing:
        s3.create_bucket(Bucket=name)
        print(f"  created bucket: {name}")
    else:
        print(f"  bucket already exists: {name}")


def ensure_queue(sqs, name: str, redrive_to_arn: str | None = None, max_receive_count: int = 3) -> str:
    attributes = {}
    if redrive_to_arn:
        import json
        attributes["RedrivePolicy"] = json.dumps(
            {"deadLetterTargetArn": redrive_to_arn, "maxReceiveCount": str(max_receive_count)}
        )
    try:
        url = sqs.get_queue_url(QueueName=name)["QueueUrl"]
        if attributes:
            sqs.set_queue_attributes(QueueUrl=url, Attributes=attributes)
        print(f"  queue already exists: {name}")
        return url
    except sqs.exceptions.QueueDoesNotExist:
        url = sqs.create_queue(QueueName=name, Attributes=attributes)["QueueUrl"]
        print(f"  created queue: {name}")
        return url


def ensure_topic(sns, name: str) -> str:
    arn = sns.create_topic(Name=name)["TopicArn"]  # create_topic is idempotent by name
    return arn


def ensure_subscription(sns, sqs, topic_arn: str, queue_url: str) -> None:
    """Subscribes an SQS queue to an SNS topic with raw message delivery,
    so consumers read the original event body directly instead of an
    SNS-wrapped envelope. Idempotent: SNS de-dupes identical subscriptions
    by (topic, protocol, endpoint)."""
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    existing = sns.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
    if any(s["Endpoint"] == queue_arn for s in existing):
        print(f"  subscription already exists: {queue_arn}")
        return
    resp = sns.subscribe(
        TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn,
        Attributes={"RawMessageDelivery": "true"},
    )
    print(f"  subscribed {queue_arn} -> {topic_arn} ({resp['SubscriptionArn']})")


def ensure_table(dynamodb, table_name: str, key_name: str) -> None:
    existing = dynamodb.list_tables()["TableNames"]
    if table_name in existing:
        print(f"  table already exists: {table_name}")
        return
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": key_name, "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": key_name, "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    print(f"  created table: {table_name}")


def main() -> None:
    print("S3 buckets:")
    s3 = aws.client("s3")
    for bucket in BUCKETS:
        ensure_bucket(s3, bucket)

    print("SQS queues:")
    sqs = aws.client("sqs")
    dlq_url = ensure_queue(sqs, DLQ_NAME)
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    validation_url = ensure_queue(sqs, VALIDATION_QUEUE, redrive_to_arn=dlq_arn)
    audit_url = ensure_queue(sqs, AUDIT_QUEUE, redrive_to_arn=dlq_arn)

    print("SNS topic + subscriptions:")
    sns = aws.client("sns")
    topic_arn = ensure_topic(sns, TOPIC_NAME)
    print(f"  topic ready: {topic_arn}")
    ensure_subscription(sns, sqs, topic_arn, validation_url)
    ensure_subscription(sns, sqs, topic_arn, audit_url)

    print("DynamoDB tables:")
    ddb = aws.client("dynamodb")
    ensure_table(ddb, IDEMPOTENCY_TABLE, "idempotency_key")
    ensure_table(ddb, METRICS_TABLE, "metric_id")
    ensure_table(ddb, JOBS_TABLE, "job_id")

    print("Bootstrap complete.")


if __name__ == "__main__":
    main()
