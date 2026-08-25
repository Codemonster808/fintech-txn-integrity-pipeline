#!/usr/bin/env python3
"""
Generate synthetic transaction events with deliberately injected duplicate
sends (same idempotency_key, different txn_id / timing) to simulate a
producer that retries on ambiguous ack failures.
"""
import argparse
import json
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common.synth import seeded_rng  # noqa: E402

SCHEMA_VERSION = 1
CURRENCIES = ["USD", "EUR", "MXN", "COP"]


def gen_event(rng, account_id: str, ts: datetime) -> dict:
    return {
        "txn_id": str(uuid.uuid4()),
        "idempotency_key": str(uuid.uuid4()),
        "account_id": account_id,
        "amount_cents": rng.randint(100, 500_000),
        "currency": rng.choice(CURRENCIES),
        "schema_version": SCHEMA_VERSION,
        "ts": ts.isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--count", type=int, default=100_000)
    parser.add_argument("--retry-rate", type=float, default=0.08)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    rng = seeded_rng(args.seed)
    accounts = [f"acc_{i:06d}" for i in range(2000)]
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    n_originals = int(args.count * (1 - args.retry_rate))
    n_retries = args.count - n_originals

    events = []
    for i in range(n_originals):
        ts = start + timedelta(seconds=i * 3)
        events.append(gen_event(rng, rng.choice(accounts), ts))

    # Retries: same idempotency_key as a prior event, new txn_id and later ts —
    # this is what the gate in src/gate/ must catch and reject with 409.
    retry_sources = rng.sample(events, min(n_retries, len(events)))
    for src in retry_sources:
        retry = dict(src)
        retry["txn_id"] = str(uuid.uuid4())
        retry_ts = datetime.fromisoformat(src["ts"]) + timedelta(seconds=rng.randint(1, 30))
        retry["ts"] = retry_ts.isoformat()
        events.append(retry)

    rng.shuffle(events)

    with out_path.open("w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

    n_duplicate_keys = len(retry_sources)
    print(f"wrote {len(events)} events to {out_path}")
    print(f"  originals: {n_originals}, injected retries: {n_duplicate_keys} "
          f"({n_duplicate_keys / len(events):.1%} of total)")


if __name__ == "__main__":
    main()
