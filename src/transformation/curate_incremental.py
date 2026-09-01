#!/usr/bin/env python3
"""
Incremental, bounded-footprint dedup — an alternative to curate.py's
global shuffle for volumes too large to shuffle in one pass on this
machine.

curate.py dedupes by shuffling the ENTIRE dataset against itself
(Window.partitionBy("idempotency_key")) — a shuffle whose size grows
with total data volume. Extrapolated from the measured 10M-row run
(benchmarks/scale-results.json, via scale_bench.py): ~420 GB of shuffle
write at 1 TB, against 50 GB of free disk on this machine. That's not
"slow" — it doesn't fit, at any speed, on this hardware.

This module processes BOUNDED batches instead (default: the size
already verified safe — 10M rows, ~926 MB shuffle, zero spill) and
checks each batch's keys against a PERSISTENT external store
(DynamoDB table txn-curated-keys) rather than against the rest of the
dataset. The gate already does exactly this in real time (a
conditional PutItem per event, no shuffle) — this brings the same
principle to the batch layer: per-batch resource use stays flat no
matter how many batches run, because "have I seen this key before"
lives outside local disk.

Cross-batch checks run via Spark's mapPartitions, NOT a driver-side
.collect() — each partition's executor builds its own boto3 client and
calls DynamoDB directly. Collecting keys to the driver would make
driver memory grow with batch size, quietly reintroducing the same
"footprint grows with volume" problem this module exists to avoid.
mapPartitions also means the DynamoDB calls genuinely parallelize
across partitions — on local[N] that's N threads; on a real cluster
it's N executors on different machines, which is the actual scaling
story here.

Two-phase writes, not one atomic conditional write, because
BatchWriteItem does not support ConditionExpression (and
TransactWriteItems fails the WHOLE batch if any single condition
fails — unusable when ~8% of a batch is expected to collide by
design):
  1. A Bloom filter (in-process, no network — see BloomFilter below),
     rebuilt and rebroadcast after each batch, cheaply rules out most
     definitely-new keys without touching DynamoDB at all.
  2. BatchGetItem (100 keys/call) resolves whatever the Bloom filter
     couldn't rule out.
  3. New rows get written to S3; BatchWriteItem (25 items/call)
     registers their keys.
This is at-least-once for the curated layer, not exactly-once — the
same trade-off already accepted by the outbox pattern
(src/orchestration/outbox_publisher.py). A duplicate row here is recoverable; the
batch job is the only writer of txn-curated-keys, so this doesn't
affect the gate's real-time exactly-once guarantee on txn-idempotency.

Known simplification, stated rather than hidden: UnprocessedKeys /
UnprocessedItems from BatchGetItem/BatchWriteItem (DynamoDB's partial-
throttling response) are not retried here. At the batch sizes this
module is designed for, MiniStack has not been observed to return
them — a production version targeting real AWS at sustained high
throughput would need a retry loop with backoff on those fields.

Usage:
    python3 src/transformation/curate_incremental.py --n-batches 10 --batch-rows 2000000
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import time
from datetime import UTC
from pathlib import Path

CURATED_KEYS_TABLE = "txn-curated-keys"
DEFAULT_BATCH_ROWS = 10_000_000  # the size already verified safe in scale_bench.py


class BloomFilter:
    """Minimal, dependency-free Bloom filter: a bytearray of bits plus
    double hashing (two real hash functions combined to derive k
    positions, the standard technique to avoid k separate hash calls
    per key). No external library — this is small enough, and specific
    enough to this use case, that a dependency wasn't worth it."""

    def __init__(self, expected_items: int, false_positive_rate: float = 0.01):
        m = max(8, int(-(expected_items * math.log(false_positive_rate)) / (math.log(2) ** 2)))
        self.size = m
        self.k = max(1, round((self.size / max(1, expected_items)) * math.log(2)))
        self.bits = bytearray((self.size // 8) + 1)

    def _positions(self, key: str):
        h1 = int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], "big")
        h2 = int.from_bytes(hashlib.md5(key.encode()).digest()[:8], "big")
        for i in range(self.k):
            yield (h1 + i * h2) % self.size

    def add(self, key: str) -> None:
        for pos in self._positions(key):
            self.bits[pos // 8] |= 1 << (pos % 8)

    def might_contain(self, key: str) -> bool:
        return all(self.bits[pos // 8] & (1 << (pos % 8)) for pos in self._positions(key))


def build_spark(app_name: str = "curate-incremental"):
    from pyspark.sql import SparkSession

    endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.5.0")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def _make_partition_filter(
    endpoint: str, region: str, access_key: str, secret_key: str, bloom_snapshot: BloomFilter
):
    """Returns a function to run once per Spark partition. Captures plain
    strings (not a boto3 client, which can't be serialized to executors)
    and a Bloom filter snapshot, and builds its own DynamoDB client
    locally when the partition actually runs."""

    def _filter_partition(rows_iter):
        import boto3

        rows = list(rows_iter)
        if not rows:
            return iter([])

        # Bloom filter first: a "definitely not seen" verdict skips the
        # DynamoDB read for that key entirely. A "maybe seen" verdict
        # (true positive OR false positive) still needs the real check.
        maybe_seen = [r for r in rows if bloom_snapshot.might_contain(r["idempotency_key"])]
        definitely_new = [r for r in rows if not bloom_snapshot.might_contain(r["idempotency_key"])]

        ddb = boto3.client(
            "dynamodb",
            endpoint_url=endpoint,
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        existing_keys = set()
        maybe_keys = [r["idempotency_key"] for r in maybe_seen]
        for i in range(0, len(maybe_keys), 100):
            chunk = maybe_keys[i : i + 100]
            resp = ddb.batch_get_item(
                RequestItems={
                    CURATED_KEYS_TABLE: {"Keys": [{"idempotency_key": {"S": k}} for k in chunk]}
                }
            )
            for item in resp.get("Responses", {}).get(CURATED_KEYS_TABLE, []):
                existing_keys.add(item["idempotency_key"]["S"])

        new_rows = definitely_new + [
            r for r in maybe_seen if r["idempotency_key"] not in existing_keys
        ]

        new_keys = [r["idempotency_key"] for r in new_rows]
        for i in range(0, len(new_keys), 25):
            chunk = new_keys[i : i + 25]
            ddb.batch_write_item(
                RequestItems={
                    CURATED_KEYS_TABLE: [
                        {"PutRequest": {"Item": {"idempotency_key": {"S": k}}}} for k in chunk
                    ]
                }
            )

        return iter(new_rows)

    return _filter_partition


def process_batch(
    spark, batch_id: int, n_rows: int, bloom: BloomFilter, cross_batch_sample: list[str]
) -> dict:
    import sys

    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
    from scale_bench import (
        synthetic_events_df,  # reuses the verified ~8% intra-batch collision generator
    )

    endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
    region = os.environ.get("AWS_REGION", "us-east-1")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "test")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")

    t0 = time.perf_counter()
    df = synthetic_events_df(spark, n_rows)

    # synthetic_events_df uses fixed seeds and always generates row_id in
    # [0, n_rows) — called with the same n_rows across batches, every batch
    # would otherwise produce the SAME key space and collide almost
    # entirely with every other batch. That's not a controlled test of
    # cross-batch dedup, it's an accident. Salt each batch's keys with its
    # batch_id so batches are genuinely independent by default — the ONLY
    # intentional overlap is the small, controlled replay injected below.
    df = df.withColumn(
        "idempotency_key", F.concat(F.lit(f"b{batch_id}-"), F.col("idempotency_key"))
    )

    # Deliberately reintroduce a handful of keys from an EARLIER batch —
    # this is what a purely local (curate.py-style) dedup could never
    # catch, since it never sees a prior batch's data. Proves the
    # external-store check does real cross-batch work, not just
    # intra-batch dedup with extra steps.
    #
    # Built as explicit extra rows UNIONed onto the batch, not by
    # rewriting existing rows in place — an earlier version tried
    # targeting specific rows via monotonically_increasing_id() and a
    # broadcast lookup, and the caught-duplicate count didn't match the
    # intended replay count (11 and 47 caught against an intended 10,
    # both runs) closely enough to trust *why* it worked, only that it
    # sort of did. A predictable count you can verify by construction
    # beats a clever mechanism you have to trust.
    n_replays = min(len(cross_batch_sample), max(1, n_rows // 5000)) if cross_batch_sample else 0
    if n_replays:
        from datetime import datetime

        from pyspark.sql import Row

        replay_ts = datetime(2026, 1, 1, tzinfo=UTC)
        replay_rows = [
            Row(
                idempotency_key=k,
                txn_id=f"replay-{batch_id}-{i}",
                account_id="acc_replay",
                amount_cents=100,
                currency="USD",
                schema_version=1,
                ts=replay_ts,
            )
            for i, k in enumerate(cross_batch_sample[:n_replays])
        ]
        replay_df = spark.createDataFrame(replay_rows, schema=df.schema)
        df = df.unionByName(replay_df)

    # Phase A: intra-batch dedup — bounded by batch size, same window
    # pattern as curate.py, just scoped to one batch instead of everything.
    window = Window.partitionBy("idempotency_key").orderBy(F.col("ts").asc())
    local_deduped = (
        df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")
    ).cache()
    n_before_cross_batch = local_deduped.count()

    # Phase B: cross-batch dedup against txn-curated-keys, via mapPartitions
    # (never a driver-side collect — see module docstring).
    #
    # .cache() here is load-bearing, not an optimization. mapPartitions is
    # lazy, and _filter_partition has a side effect (BatchWriteItem
    # registers each new key in DynamoDB). Without caching, new_df gets
    # re-evaluated once per action below (.write, .count(), .select().
    # collect()) — three full re-runs of the SAME transformation, so the
    # first run's writes become visible to the second run's BatchGetItem
    # reads, which then wrongly treats its own prior pass's output as
    # pre-existing duplicates. Caught by comparing per-partition debug
    # counts across the three evaluations and finding they grew between
    # passes instead of matching — not by assuming a single mapPartitions
    # call meant a single evaluation.
    partition_filter = _make_partition_filter(endpoint, region, access_key, secret_key, bloom)
    new_rdd = local_deduped.rdd.mapPartitions(partition_filter)
    new_df = spark.createDataFrame(new_rdd, local_deduped.schema).cache()

    output_path = f"s3a://txn-curated/incremental/batch={batch_id:05d}/"
    new_df.write.mode("overwrite").parquet(output_path)
    n_after_cross_batch = new_df.count()
    elapsed_s = time.perf_counter() - t0

    # Every genuinely new key MUST be added to the Bloom filter, not a
    # capped sample — a filter that only knows about some of the keys
    # actually written to DynamoDB would wrongly report "definitely not
    # seen" for the ones it was never told about, letting them bypass the
    # DynamoDB check entirely on a later batch and reappear as false
    # "new" rows. This collect is bounded by THIS batch's new-row count
    # (at most n_rows, same order of magnitude as the batch itself) — it
    # does not grow with the number of batches already processed, which
    # is what keeps the "flat footprint" claim true. It is a real,
    # deliberate cost of maintaining an in-process Bloom filter across
    # batches within one run; documented here rather than hidden.
    new_keys_all = [row["idempotency_key"] for row in new_df.select("idempotency_key").collect()]
    for k in new_keys_all:
        bloom.add(k)

    # Release both cached DataFrames — without this, memory held by
    # .cache() accumulates across the batch loop in run(), which would
    # quietly break the "flat footprint per batch" claim over many batches.
    new_df.unpersist()
    local_deduped.unpersist()

    return {
        "batch_id": batch_id,
        "batch_rows": n_rows,
        "rows_after_intra_batch_dedup": n_before_cross_batch,
        "rows_new_after_cross_batch_check": n_after_cross_batch,
        "cross_batch_duplicates_caught": n_before_cross_batch - n_after_cross_batch,
        "elapsed_seconds": round(elapsed_s, 3),
        "rows_per_second": round(n_rows / elapsed_s, 1) if elapsed_s > 0 else None,
        # Small sample, unrelated to the Bloom filter — only feeds the next
        # batch's deliberate cross-batch replay test (see call site above).
        "new_keys_sample": new_keys_all[:50],
    }


def ensure_curated_keys_table() -> None:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from utils import aws

    ddb = aws.client("dynamodb")
    existing = ddb.list_tables()["TableNames"]
    if CURATED_KEYS_TABLE in existing:
        return
    ddb.create_table(
        TableName=CURATED_KEYS_TABLE,
        KeySchema=[{"AttributeName": "idempotency_key", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "idempotency_key", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    print(f"  created table: {CURATED_KEYS_TABLE}")


def run(n_batches: int, batch_rows: int, json_out: str) -> list[dict]:
    ensure_curated_keys_table()
    spark = build_spark()
    bloom = BloomFilter(expected_items=n_batches * batch_rows, false_positive_rate=0.01)

    results = []
    prior_sample: list[str] = []
    try:
        for batch_id in range(n_batches):
            print(f"--- batch {batch_id + 1}/{n_batches} ({batch_rows:,} rows) ---")
            result = process_batch(spark, batch_id, batch_rows, bloom, prior_sample)
            print(json.dumps({k: v for k, v in result.items() if k != "new_keys_sample"}, indent=2))
            prior_sample = result["new_keys_sample"]
            results.append(result)
    finally:
        spark.stop()

    Path(json_out).parent.mkdir(parents=True, exist_ok=True)
    Path(json_out).write_text(json.dumps(results, indent=2))
    print(f"\nwrote {json_out}")
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n-batches", type=int, default=10)
    parser.add_argument("--batch-rows", type=int, default=DEFAULT_BATCH_ROWS)
    parser.add_argument("--json-out", default="benchmarks/incremental-results.json")
    args = parser.parse_args()
    run(args.n_batches, args.batch_rows, args.json_out)


if __name__ == "__main__":
    main()
