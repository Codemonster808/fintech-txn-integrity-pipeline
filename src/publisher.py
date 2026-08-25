#!/usr/bin/env python3
"""
Publishes synthetic transaction events from a JSONL file to the SNS
topic — the producer side of the `producers -> SNS -> SQS` flow the
README describes. This is the piece `make demo` was missing: it used to
reference a `src/replay.py` that never existed.
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import aws  # noqa: E402

TOPIC_NAME = "txn-events"


def publish_file(sns, topic_arn: str, path: str) -> int:
    n = 0
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            sns.publish(TopicArn=topic_arn, Message=line)
            n += 1
    return n


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="in_path", required=True)
    args = parser.parse_args()

    sns = aws.client("sns")
    topic_arn = sns.create_topic(Name=TOPIC_NAME)["TopicArn"]

    n = publish_file(sns, topic_arn, args.in_path)
    print(f"published {n} events to {topic_arn}")


if __name__ == "__main__":
    main()
