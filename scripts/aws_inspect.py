#!/usr/bin/env python3
"""Inspect MiniStack state after each pipeline step.

    source env.sh
    python3 scripts/aws_inspect.py all
    python3 scripts/aws_inspect.py s3
    python3 scripts/aws_inspect.py ddb
    python3 scripts/aws_inspect.py sqs
    python3 scripts/aws_inspect.py sns
    python3 scripts/aws_inspect.py lambda
    python3 scripts/aws_inspect.py sfn
    python3 scripts/aws_inspect.py ecs

Reads resource names from scripts/resources.json in this repo. A section
only prints if this repo's resources.json declares that resource type —
not every repo uses every service.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from common import aws  # noqa: E402

CFG = json.loads((ROOT / "scripts" / "resources.json").read_text())


def _ok(msg: str) -> None:
    print(f"  {msg}")


def inspect_s3() -> None:
    s3 = aws.client("s3")
    print("S3 buckets")
    for bucket in CFG.get("buckets", []):
        try:
            resp = s3.list_objects_v2(Bucket=bucket, MaxKeys=8)
        except Exception as e:
            print(f"  {bucket}: ERROR {e}")
            continue
        n = resp.get("KeyCount", 0)
        keys = [o["Key"] for o in resp.get("Contents", [])]
        print(f"  {bucket}: {n} object(s) (showing up to 8)")
        for k in keys:
            print(f"      {k}")


def inspect_ddb() -> None:
    ddb = aws.client("dynamodb")
    print("DynamoDB tables")
    for table in CFG.get("tables", []):
        try:
            desc = ddb.describe_table(TableName=table)["Table"]
            scan = ddb.scan(TableName=table, Limit=5)
        except Exception as e:
            print(f"  {table}: ERROR {e}")
            continue
        count = desc.get("ItemCount", "?")
        print(f"  {table}: ItemCount≈{count}, showing {len(scan.get('Items', []))} item(s)")
        for item in scan.get("Items", []):
            compact = {k: list(v.values())[0] for k, v in item.items()}
            print(f"      {compact}")


def inspect_sqs() -> None:
    sqs = aws.client("sqs")
    print("SQS queues")
    for name in CFG.get("queues", []):
        try:
            url = sqs.get_queue_url(QueueName=name)["QueueUrl"]
            attrs = sqs.get_queue_attributes(
                QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
            )["Attributes"]
        except Exception as e:
            print(f"  {name}: ERROR {e}")
            continue
        visible = attrs.get("ApproximateNumberOfMessages", "?")
        in_flight = attrs.get("ApproximateNumberOfMessagesNotVisible", "?")
        print(f"  {name}: visible={visible} in_flight={in_flight}")


def inspect_sns() -> None:
    sns = aws.client("sns")
    print("SNS topics")
    topics = CFG.get("topics", [])
    if not topics:
        print("  (none declared in scripts/resources.json)")
        return
    for name in topics:
        try:
            topic_arn = sns.create_topic(Name=name)["TopicArn"]  # idempotent lookup-by-name
            subs = sns.list_subscriptions_by_topic(TopicArn=topic_arn).get("Subscriptions", [])
        except Exception as e:
            print(f"  {name}: ERROR {e}")
            continue
        print(f"  {name}: {topic_arn}")
        if not subs:
            print("      (no subscriptions)")
        for sub in subs:
            print(f"      -> {sub['Protocol']}:{sub['Endpoint']}")


def inspect_lambda() -> None:
    lam = aws.client("lambda")
    print("Lambda functions")
    functions = CFG.get("functions", [])
    if not functions:
        print("  (none declared in scripts/resources.json)")
        return
    for name in functions:
        try:
            cfg = lam.get_function(FunctionName=name)["Configuration"]
        except Exception as e:
            print(f"  {name}: not deployed yet ({e.__class__.__name__})")
            continue
        print(f"  {name}: state={cfg['State']} runtime={cfg['Runtime']} last_modified={cfg['LastModified']}")


def inspect_ecs() -> None:
    ecs = aws.client("ecs")
    print("ECS")
    clusters = CFG.get("clusters", [])
    if not clusters:
        print("  (none declared in scripts/resources.json)")
        return
    for cluster in clusters:
        try:
            tasks_arns = ecs.list_tasks(cluster=cluster).get("taskArns", [])
        except Exception as e:
            print(f"  {cluster}: not created yet ({e.__class__.__name__})")
            continue
        print(f"  cluster {cluster}: {len(tasks_arns)} task(s)")
        if tasks_arns:
            descs = ecs.describe_tasks(cluster=cluster, tasks=tasks_arns)["tasks"]
            for t in descs:
                print(f"      {t['lastStatus']:10}  {t['taskArn'].rsplit('/', 1)[-1]}")


def inspect_sfn() -> None:
    sfn = aws.client("stepfunctions")
    print("Step Functions")
    try:
        machines = sfn.list_state_machines().get("stateMachines", [])
    except Exception as e:
        print(f"  ERROR {e}")
        return
    if not machines:
        print("  (none deployed yet — run src/statemachine.py)")
        return
    for sm in machines:
        print(f"  {sm['name']}")
        execs = sfn.list_executions(stateMachineArn=sm["stateMachineArn"], maxResults=5).get("executions", [])
        if not execs:
            print("      (no executions yet)")
        for ex in execs:
            print(f"      {ex['status']:12}  {ex['name']}")


HANDLERS = {
    "s3": inspect_s3,
    "ddb": inspect_ddb,
    "sqs": inspect_sqs,
    "sns": inspect_sns,
    "lambda": inspect_lambda,
    "sfn": inspect_sfn,
    "ecs": inspect_ecs,
}


def main() -> None:
    what = (sys.argv[1] if len(sys.argv) > 1 else "all").lower()
    if what == "all":
        for fn in HANDLERS.values():
            fn()
            print()
        return
    if what not in HANDLERS:
        print(f"usage: python3 scripts/aws_inspect.py [{' | '.join(['all', *HANDLERS])}]", file=sys.stderr)
        sys.exit(2)
    HANDLERS[what]()


if __name__ == "__main__":
    main()
