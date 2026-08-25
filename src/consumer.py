#!/usr/bin/env python3
"""
Polls the validation queue (fed by SNS), sends each event through the Go
idempotency gate, and — for events the gate accepts — runs schema
validation to route them to raw or quarantine. Closes the
producer -> SNS -> SQS -> gate -> validator -> S3 path end to end.

Duplicates rejected by the gate (409) are simply acknowledged and
dropped from the queue — they were already correctly handled by not
being written anywhere.
"""
import argparse
import json
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import aws  # noqa: E402
from validator import handler as validate_event  # noqa: E402

VALIDATION_QUEUE = "txn-validation-queue"
GATE_URL = "http://localhost:8080"


def process_message(body: str) -> dict:
    event = json.loads(body)
    gate_resp = requests.post(f"{GATE_URL}/accept", json=event, timeout=5)

    if gate_resp.status_code == 409:
        return {"outcome": "duplicate_rejected", "idempotency_key": event.get("idempotency_key")}
    if gate_resp.status_code != 200:
        raise RuntimeError(f"gate returned unexpected status {gate_resp.status_code}: {gate_resp.text}")

    validation = validate_event(event)
    return {"outcome": "processed", "validation": validation}


def run(max_messages: int | None = None, idle_timeout_s: float = 5.0) -> dict:
    sqs = aws.client("sqs")
    queue_url = sqs.get_queue_url(QueueName=VALIDATION_QUEUE)["QueueUrl"]

    stats = {"processed": 0, "duplicate_rejected": 0, "errors": 0}
    last_message_at = time.time()

    while True:
        if max_messages is not None and stats["processed"] + stats["duplicate_rejected"] >= max_messages:
            break
        if time.time() - last_message_at > idle_timeout_s:
            break

        resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1)
        messages = resp.get("Messages", [])
        if not messages:
            continue

        for msg in messages:
            last_message_at = time.time()
            try:
                result = process_message(msg["Body"])
                stats[result["outcome"]] = stats.get(result["outcome"], 0) + 1
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
            except Exception as e:
                stats["errors"] += 1
                print(f"error processing message: {e}", file=sys.stderr)
                # leave the message in the queue; SQS redelivers it, and the
                # redrive policy sends it to the DLQ after maxReceiveCount

    return stats


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--idle-timeout", type=float, default=5.0)
    args = parser.parse_args()

    stats = run(max_messages=args.max_messages, idle_timeout_s=args.idle_timeout)
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
