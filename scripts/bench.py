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


def bench_gate_throughput_concurrent(n: int, concurrency: int) -> dict:
    """Real gate capacity, not a serial client's throughput.

    bench_gate_latency() above sends one request, waits for the response,
    then sends the next — its "requests/second" is 1000/p50_ms, which is
    the throughput of a SERIAL CALLER, not the gate. Gin (like most HTTP
    servers) handles concurrent connections natively; the gate's actual
    capacity was never measured until this function existed. The
    "~273 events/s" figure quoted elsewhere in this repo's docs is
    explicitly the single-threaded number — this is the one to use for
    any claim about the gate's real ceiling.
    """
    import concurrent.futures

    def send_one(_i: int) -> tuple[int, float]:
        event = {
            "txn_id": str(uuid.uuid4()),
            "idempotency_key": str(uuid.uuid4()),
            "account_id": "acc_bench",
            "amount_cents": 100,
            "currency": "USD",
            "schema_version": 1,
            "ts": "2026-01-01T00:00:00Z",
        }
        t0 = time.perf_counter()
        resp = requests.post(f"{GATE_URL}/accept", json=event, timeout=10)
        return resp.status_code, (time.perf_counter() - t0) * 1000

    t_start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as ex:
        results = list(ex.map(send_one, range(n)))
    total_s = time.perf_counter() - t_start

    latencies_ms = sorted(elapsed for _, elapsed in results)
    n_ok = sum(1 for status, _ in results if status == 200)

    return {
        "n_requests": n,
        "concurrency": concurrency,
        "total_seconds": round(total_s, 3),
        "throughput_rps": round(n / total_s, 1) if total_s > 0 else None,
        "p50_ms": round(latencies_ms[len(latencies_ms) // 2], 2),
        "p95_ms": round(latencies_ms[int(len(latencies_ms) * 0.95)], 2),
        "n_200": n_ok,
        "n_non_200": n - n_ok,
    }


def bench_gate_saturation_curve(
    n_per_level: int = 500, levels: tuple[int, ...] = (1, 8, 16, 32, 64)
) -> list[dict]:
    """The curve, not a single number — throughput at concurrency=1 should
    roughly match the serial benchmark above; where it stops climbing as
    concurrency increases is the gate's real saturation point."""
    return [bench_gate_throughput_concurrent(n_per_level, c) for c in levels]


def bench_dedup_rate() -> dict:
    resp = requests.get(f"{API_URL}/metrics/dedup")
    return resp.json()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="benchmarks/results.json")
    parser.add_argument("--n", type=int, default=200)
    parser.add_argument(
        "--concurrent",
        action="store_true",
        help="also measure the concurrent saturation curve (benchmarks/gate-throughput.json)",
    )
    parser.add_argument("--concurrent-n-per-level", type=int, default=500)
    parser.add_argument("--concurrent-out", default="benchmarks/gate-throughput.json")
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

    if args.concurrent:
        print(
            f"\nbenchmarking concurrent saturation curve "
            f"({args.concurrent_n_per_level} requests/level)..."
        )
        curve = bench_gate_saturation_curve(n_per_level=args.concurrent_n_per_level)
        for level in curve:
            print(json.dumps(level, indent=2))

        concurrent_out_path = Path(args.concurrent_out)
        concurrent_out_path.parent.mkdir(parents=True, exist_ok=True)
        concurrent_out_path.write_text(json.dumps({"saturation_curve": curve}, indent=2))
        print(f"\nwrote {concurrent_out_path}")


if __name__ == "__main__":
    main()
