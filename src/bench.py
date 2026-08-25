#!/usr/bin/env python3
"""Produces benchmarks/results.json — the numbers that go in the README's
'Measured in this repo' table. Assumes the gate is running on :8080 and
MiniStack is up with bootstrap.py already run."""
import argparse
import json
import statistics
import time
import uuid
from pathlib import Path

import requests

GATE_URL = "http://localhost:8080"
API_URL = "http://localhost:8000"


def bench_gate_latency(n: int = 200) -> dict:
    latencies_ms = []
    for _ in range(n):
        event = {
            "txn_id": str(uuid.uuid4()),
            "idempotency_key": str(uuid.uuid4()),
            "account_id": "acc_bench",
            "amount_cents": 100,
            "currency": "USD",
            "schema_version": 1,
            "ts": "2026-01-01T00:00:00Z",
        }
        start = time.perf_counter()
        resp = requests.post(f"{GATE_URL}/accept", json=event)
        latencies_ms.append((time.perf_counter() - start) * 1000)
        assert resp.status_code == 200

    latencies_ms.sort()
    p50 = latencies_ms[len(latencies_ms) // 2]
    p95 = latencies_ms[int(len(latencies_ms) * 0.95)]
    return {
        "n_requests": n,
        "p50_ms": round(p50, 2),
        "p95_ms": round(p95, 2),
        "mean_ms": round(statistics.mean(latencies_ms), 2),
    }


def bench_dedup_rate() -> dict:
    resp = requests.get(f"{API_URL}/metrics/dedup")
    return resp.json()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="benchmarks/results.json")
    parser.add_argument("--n", type=int, default=200)
    args = parser.parse_args()

    print(f"benchmarking gate latency over {args.n} requests...")
    gate_stats = bench_gate_latency(args.n)

    print("reading dedup metrics from API...")
    dedup_stats = bench_dedup_rate()

    results = {
        "gate_latency": gate_stats,
        "dedup": dedup_stats,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2))

    print(json.dumps(results, indent=2))
    print(f"\nwrote {out_path}")


if __name__ == "__main__":
    main()
