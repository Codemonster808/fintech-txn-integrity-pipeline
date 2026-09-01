#!/usr/bin/env python3
"""
Scale benchmark for the curation path (the same dedup-by-idempotency-key
shuffle as src/transformation/curate.py), plus a "logical" mode that processes billions
of in-flight rows without ever touching disk.

Two modes, because a literal 1 TB run is not possible on this machine —
verified, not assumed:
  - 1 TB of 237-byte events = 4,639,289,569 rows.
  - This machine has 50 GB free disk. A dedup-by-key shuffle at 1 TB
    needs on the order of 1 TB of shuffle spill. It does not fit.
  - The measured gate throughput (benchmarks/results.json, p50=3.66ms
    single-threaded) is ~273 events/s. At that rate, 1 TB would take
    ~5.4 months through the gate alone — the gate, not Spark, is the
    real bottleneck at scale (see `report()`'s bottleneck comparison).

    materialized  Writes real Parquet at increasing row counts, measures
                  wall time per phase and Spark's own shuffle/spill
                  metrics (via the Spark UI REST API), then extrapolates
                  to 1 TB with the assumptions stated in the output —
                  never silently.

    logical       spark.range(N) generates rows in memory; only a
                  filter+projection+low-cardinality aggregate runs, no
                  shuffle, nothing written to S3. This is how the run
                  reaches billions of rows within a sane time budget —
                  reported honestly as "logical rows processed
                  in-flight," not as materialized throughput.

Usage:
    python3 scripts/scale_bench.py --mode materialized --scales 10000,100000,1000000,10000000
    python3 scripts/scale_bench.py --mode logical --rows 2000000000
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import requests

ONE_TB_BYTES = 1024**4
MEASURED_EVENT_BYTES = 237  # actual size of one data_gen.py event, measured this session
ONE_TB_ROWS = ONE_TB_BYTES // MEASURED_EVENT_BYTES
FREE_DISK_BYTES = 50 * 1024**3  # measured this session; re-check with `df -h` if this drifts

GATE_RESULTS_PATH = Path(__file__).resolve().parents[1] / "benchmarks" / "results.json"


def build_spark(app_name: str = "scale-bench"):
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


def synthetic_events_df(spark, n: int):
    """Builds n synthetic transaction events entirely inside Spark
    (spark.range + column expressions) — generating them in a Python
    loop first would make data_gen.py the bottleneck of this benchmark
    at these row counts, not the pipeline being measured. ~8% of rows
    deliberately collide on idempotency_key (same distribution as
    data_gen.py's --retry-rate 0.08), so the dedup shuffle this
    benchmark measures does real work, not a no-op over unique keys."""
    from pyspark.sql import functions as F

    df = spark.range(0, n).withColumnRenamed("id", "row_id")
    df = df.withColumn(
        "idempotency_key",
        F.when(
            F.rand(seed=42) < 0.08,
            # Retry rows reuse an EARLIER row's key: a uniformly random
            # row_id strictly less than this one, via floor(rand()*row_id)
            # — NOT `row_id % n_orig`, which was tried first and turned out
            # to be an identity map for ~92% of rows (any row_id already
            # smaller than the modulus maps to itself), so flagging those
            # rows "retry" silently didn't change their key at all and
            # produced ~0.8% duplicates instead of the intended ~8%. Found
            # by checking duplicates_dropped on the smoke test — it didn't
            # match the injected rate — not by assuming the expression was
            # right because it looked reasonable.
            F.concat(
                F.lit("orig-"),
                F.floor(F.rand(seed=99) * F.greatest(F.col("row_id"), F.lit(1))).cast("string"),
            ),
        ).otherwise(F.concat(F.lit("orig-"), F.col("row_id").cast("string"))),
    )
    df = df.withColumn(
        "txn_id",
        F.sha2(
            F.concat(F.lit("txn-"), F.col("row_id").cast("string"), F.rand(seed=7).cast("string")),
            256,
        ),
    )
    df = df.withColumn(
        "account_id", F.concat(F.lit("acc_"), (F.col("row_id") % 2000).cast("string"))
    )
    df = df.withColumn("amount_cents", (F.rand(seed=11) * 499900 + 100).cast("long"))
    currencies = F.array(F.lit("USD"), F.lit("EUR"), F.lit("MXN"), F.lit("COP"))
    df = df.withColumn("currency", F.element_at(currencies, (F.col("row_id") % 4 + 1).cast("int")))
    df = df.withColumn("schema_version", F.lit(1))
    df = df.withColumn(
        "ts",
        F.expr("timestamp'2026-01-01T00:00:00Z' + make_interval(0,0,0,0,0,0,row_id % 86400)"),
    )
    return df.drop("row_id")


# ---------------------------------------------------------------------------
# Spark UI REST API metrics — shuffle read/write and spill, per benchmark
# phase. Wrapped defensively: if the UI is unreachable for any reason, the
# benchmark still completes and this field is just None, matching the
# fail-visibly-not-silently pattern already used elsewhere in this repo.
# ---------------------------------------------------------------------------


def _ui_base_url(spark) -> str | None:
    url = spark.sparkContext.uiWebUrl
    return url if url else None


def _app_id(spark) -> str:
    return spark.sparkContext.applicationId


def _max_completed_stage_id(spark) -> int:
    base = _ui_base_url(spark)
    if not base:
        return -1
    try:
        resp = requests.get(
            f"{base}/api/v1/applications/{_app_id(spark)}/stages?status=complete", timeout=5
        )
        stages = resp.json()
        return max((s["stageId"] for s in stages), default=-1)
    except Exception:
        return -1


def _shuffle_metrics_since(spark, stage_id_floor: int) -> dict | None:
    base = _ui_base_url(spark)
    if not base:
        return None
    try:
        resp = requests.get(
            f"{base}/api/v1/applications/{_app_id(spark)}/stages?status=complete", timeout=5
        )
        stages = [s for s in resp.json() if s["stageId"] > stage_id_floor]
        return {
            "shuffle_read_bytes": sum(s.get("shuffleReadBytes", 0) for s in stages),
            "shuffle_write_bytes": sum(s.get("shuffleWriteBytes", 0) for s in stages),
            "disk_bytes_spilled": sum(s.get("diskBytesSpilled", 0) for s in stages),
            "memory_bytes_spilled": sum(s.get("memoryBytesSpilled", 0) for s in stages),
            "n_stages": len(stages),
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Materialized mode
# ---------------------------------------------------------------------------


def run_materialized_scale(spark, n: int) -> dict:
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    input_path = f"s3a://txn-raw/scale-bench/input/{n}/"
    output_path = f"s3a://txn-curated/scale-bench/output/{n}/"

    stage_floor = _max_completed_stage_id(spark)

    t0 = time.perf_counter()
    df = synthetic_events_df(spark, n)
    df.write.mode("overwrite").parquet(input_path)
    write_input_s = time.perf_counter() - t0

    t0 = time.perf_counter()
    read_back = spark.read.parquet(input_path)
    n_before = read_back.count()

    window = Window.partitionBy("idempotency_key").orderBy(F.col("ts").asc())
    deduped = (
        read_back.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("ingest_hour", F.date_format(F.col("ts"), "yyyy-MM-dd-HH"))
    )
    n_after = deduped.count()
    deduped.write.mode("overwrite").partitionBy("ingest_hour").parquet(output_path)
    dedup_and_write_s = time.perf_counter() - t0

    shuffle = _shuffle_metrics_since(spark, stage_floor)
    input_bytes = n * MEASURED_EVENT_BYTES

    return {
        "scale_rows": n,
        "status": "ok",
        "input_bytes_estimate": input_bytes,
        "rows_before_dedup": n_before,
        "rows_after_dedup": n_after,
        "duplicates_dropped": n_before - n_after,
        "write_input_seconds": round(write_input_s, 3),
        "dedup_and_write_output_seconds": round(dedup_and_write_s, 3),
        "total_seconds": round(write_input_s + dedup_and_write_s, 3),
        "rows_per_second_dedup_phase": round(n / dedup_and_write_s, 1)
        if dedup_and_write_s > 0
        else None,
        "shuffle_metrics": shuffle,
    }


def run_materialized(spark, scales: list[int]) -> list[dict]:
    results = []
    for n in scales:
        print(f"--- scale: {n:,} rows ---")
        try:
            result = run_materialized_scale(spark, n)
            print(json.dumps(result, indent=2))
        except Exception as e:
            result = {"scale_rows": n, "status": "failed", "reason": f"{type(e).__name__}: {e}"}
            print(f"FAILED at {n:,} rows: {result['reason']}")
        results.append(result)
    return results


# ---------------------------------------------------------------------------
# Logical mode — billions of rows, generated and consumed in-flight
# ---------------------------------------------------------------------------


def run_logical(spark, n: int) -> dict:
    from pyspark.sql import functions as F

    t0 = time.perf_counter()
    df = synthetic_events_df(spark, n)
    # Filter + projection + a low-cardinality aggregate — no shuffle on a
    # high-cardinality key, nothing written anywhere. This is what makes
    # billions of rows tractable in minutes: nothing is ever materialized.
    result = (
        df.filter(F.col("amount_cents") > 100)
        .withColumn("hour_bucket", F.date_format(F.col("ts"), "yyyy-MM-dd-HH"))
        .groupBy("hour_bucket")
        .agg(F.count("*").alias("n"), F.sum("amount_cents").alias("total_amount_cents"))
        .agg(
            F.sum("n").alias("total_rows"), F.sum("total_amount_cents").alias("total_amount_cents")
        )
        .collect()[0]
    )
    elapsed_s = time.perf_counter() - t0

    processed_rows = int(result["total_rows"])
    return {
        "requested_rows": n,
        "processed_rows": processed_rows,
        "logical_bytes_estimate": processed_rows * MEASURED_EVENT_BYTES,
        "elapsed_seconds": round(elapsed_s, 3),
        "rows_per_second": round(processed_rows / elapsed_s, 1) if elapsed_s > 0 else None,
        "materialized_to_disk": False,
    }


# ---------------------------------------------------------------------------
# Extrapolation — explicit assumptions, explicit breaking point
# ---------------------------------------------------------------------------


def extrapolate_to_1tb(materialized_results: list[dict]) -> dict:
    ok = [
        r
        for r in materialized_results
        if r.get("status") == "ok" and r.get("rows_per_second_dedup_phase")
    ]
    if not ok:
        return {"error": "no successful materialized runs to extrapolate from"}

    largest = max(ok, key=lambda r: r["scale_rows"])
    rate = largest["rows_per_second_dedup_phase"]
    seconds_at_1tb = ONE_TB_ROWS / rate

    # Extrapolate from shuffle WRITE bytes, not disk_bytes_spilled.
    # disk_bytes_spilled was 0 at every measured scale (up to 10M rows, the
    # shuffled data comfortably fits in the 2g driver) — extrapolating
    # linearly from a zero baseline would always predict zero spill at any
    # scale, which is wrong: spill is a step function that kicks in once
    # shuffle volume exceeds available memory, not a linear quantity.
    # Shuffle bytes DO scale roughly linearly with row count and were
    # nonzero at every measured scale, so they're what actually predicts
    # whether the shuffle would need to spill — and how much — at 1 TB.
    shuffle = largest.get("shuffle_metrics") or {}
    shuffle_write_per_row = (shuffle.get("shuffle_write_bytes", 0) or 0) / largest["scale_rows"]
    projected_shuffle_write_at_1tb = shuffle_write_per_row * ONE_TB_ROWS
    driver_memory_bytes = 2 * 1024**3  # matches build_spark()'s spark.driver.memory

    gate_throughput = _measured_gate_throughput()

    return {
        "assumptions": [
            "linear scaling of the largest measured rows/second past the measured range",
            "same single-machine hardware, no contention from other workloads",
            "shuffle spill scales linearly with row count (no algorithmic change)",
        ],
        "based_on_scale_rows": largest["scale_rows"],
        "measured_rows_per_second": rate,
        "one_tb_rows": ONE_TB_ROWS,
        "extrapolated_seconds_for_1tb_dedup_phase": round(seconds_at_1tb, 1),
        "extrapolated_time_for_1tb_dedup_phase_human": _human_duration(seconds_at_1tb),
        "projected_shuffle_write_bytes_at_1tb": int(projected_shuffle_write_at_1tb),
        "driver_memory_bytes": driver_memory_bytes,
        "free_disk_bytes": FREE_DISK_BYTES,
        "model_breaks_down": projected_shuffle_write_at_1tb > FREE_DISK_BYTES,
        "model_breakdown_reason": (
            f"projected shuffle volume at 1 TB ({projected_shuffle_write_at_1tb / 1024**3:.1f} GB) "
            f"is {projected_shuffle_write_at_1tb / driver_memory_bytes:.0f}x the 2 GB driver memory "
            f"and exceeds free disk ({FREE_DISK_BYTES / 1024**3:.0f} GB) — at every measured scale "
            "(up to 10M rows) disk_bytes_spilled was 0 because shuffle data fit in driver memory; "
            "at 1 TB it would not, forcing spill, and there isn't enough disk for that spill either. "
            "This is not achievable on this machine at any speed — it would require a distributed "
            "shuffle across multiple nodes, not just more time on one."
            if projected_shuffle_write_at_1tb > FREE_DISK_BYTES
            else "projected shuffle volume at 1 TB fits within free disk on this machine"
        ),
        "gate_bottleneck_comparison": gate_throughput,
    }


def _measured_gate_throughput() -> dict | None:
    if not GATE_RESULTS_PATH.exists():
        return None
    try:
        data = json.loads(GATE_RESULTS_PATH.read_text())
        p50_ms = data["gate_latency"]["p50_ms"]
        events_per_sec = 1000 / p50_ms
        seconds_at_1tb = ONE_TB_ROWS / events_per_sec
        return {
            "source": str(GATE_RESULTS_PATH),
            "measured_p50_ms": p50_ms,
            "single_threaded_events_per_second": round(events_per_sec, 1),
            "extrapolated_seconds_for_1tb_through_gate": round(seconds_at_1tb, 1),
            "extrapolated_time_for_1tb_through_gate_human": _human_duration(seconds_at_1tb),
            "note": (
                "The gate is a single synchronous HTTP hop per event — at 1 TB row counts "
                "this is the real bottleneck, orders of magnitude below Spark's dedup "
                "throughput. Scaling this pipeline to TB volumes means batching the "
                "idempotency check, not tuning Spark."
            ),
        }
    except Exception:
        return None


def _human_duration(seconds: float) -> str:
    for unit, size in (("years", 31_536_000), ("days", 86_400), ("hours", 3600), ("minutes", 60)):
        if seconds >= size:
            return f"{seconds / size:.1f} {unit}"
    return f"{seconds:.1f} seconds"


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def write_report(
    materialized_results: list[dict],
    logical_result: dict | None,
    extrapolation: dict | None,
    json_path: str,
    md_path: str,
) -> None:
    out = {
        "materialized": materialized_results,
        "logical": logical_result,
        "extrapolation_to_1tb": extrapolation,
    }
    Path(json_path).parent.mkdir(parents=True, exist_ok=True)
    Path(json_path).write_text(json.dumps(out, indent=2))

    lines = ["# Scale benchmark — fintech-txn-integrity-pipeline", ""]
    lines.append(
        f"1 TB of real events ({MEASURED_EVENT_BYTES}-byte events, measured) = **{ONE_TB_ROWS:,} rows**. "
        f"This machine has {FREE_DISK_BYTES / 1024**3:.0f} GB free disk — a literal 1 TB dedup shuffle does "
        "not fit here. What follows is measured up to the largest scale that does fit, extrapolated from "
        "there with explicit assumptions."
    )
    if materialized_results:
        lines += [
            "",
            "## Materialized curve (real Parquet, real shuffle)",
            "",
            "| Rows | Status | Write input (s) | Dedup+write output (s) | Rows/s (dedup phase) | Shuffle spill (disk) |",
            "|---|---|---|---|---|---|",
        ]
    for r in materialized_results:
        if r.get("status") == "ok":
            spill = r.get("shuffle_metrics") or {}
            spill_mb = (spill.get("disk_bytes_spilled") or 0) / 1024**2
            lines.append(
                f"| {r['scale_rows']:,} | OK | {r['write_input_seconds']} | "
                f"{r['dedup_and_write_output_seconds']} | {r['rows_per_second_dedup_phase']:,} | {spill_mb:.1f} MB |"
            )
        else:
            lines.append(
                f"| {r['scale_rows']:,} | **FAILED** | — | — | — | {r.get('reason', '')} |"
            )

    if logical_result:
        lines += [
            "",
            "## Logical mode (in-flight, nothing written to disk)",
            "",
            f"- Requested rows: {logical_result['requested_rows']:,}",
            f"- Processed rows: {logical_result['processed_rows']:,}",
            f"- Logical bytes: {logical_result['logical_bytes_estimate'] / 1024**3:.1f} GB "
            f"(equivalent {logical_result['processed_rows'] / ONE_TB_ROWS:.1%} of 1 TB)",
            f"- Elapsed: {logical_result['elapsed_seconds']}s "
            f"({logical_result['rows_per_second']:,} rows/s)",
            "- **Nothing was written to S3** — this is generated and consumed in-flight, not "
            "a materialized-throughput claim.",
        ]

    if extrapolation and "error" not in extrapolation:
        lines += [
            "",
            "## Extrapolation to 1 TB",
            "",
            f"Based on the {extrapolation['based_on_scale_rows']:,}-row measured run "
            f"({extrapolation['measured_rows_per_second']:,} rows/s, dedup phase):",
            "",
            f"- Extrapolated time for 1 TB through the dedup/curate path: "
            f"**{extrapolation['extrapolated_time_for_1tb_dedup_phase_human']}**",
            f"- Projected shuffle volume at 1 TB: "
            f"{extrapolation['projected_shuffle_write_bytes_at_1tb'] / 1024**3:.1f} GB "
            f"(free disk: {extrapolation['free_disk_bytes'] / 1024**3:.0f} GB, "
            f"driver memory: {extrapolation['driver_memory_bytes'] / 1024**3:.0f} GB)",
            f"- **Model breaks down: {extrapolation['model_breaks_down']}** — "
            f"{extrapolation['model_breakdown_reason']}",
            "",
            "Assumptions: " + "; ".join(extrapolation["assumptions"]) + ".",
        ]

        gate = extrapolation.get("gate_bottleneck_comparison")
        if gate:
            lines += [
                "",
                "### The real bottleneck isn't Spark",
                "",
                f"Measured gate latency (p50={gate['measured_p50_ms']}ms, single-threaded): "
                f"**{gate['single_threaded_events_per_second']:,} events/s**. "
                f"At that rate, 1 TB through the gate alone: "
                f"**{gate['extrapolated_time_for_1tb_through_gate_human']}** — "
                f"vs. {extrapolation['extrapolated_time_for_1tb_dedup_phase_human']} for the Spark "
                "dedup phase on the same row count. " + gate["note"],
            ]

    Path(md_path).parent.mkdir(parents=True, exist_ok=True)
    Path(md_path).write_text("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["materialized", "logical"], required=True)
    parser.add_argument(
        "--scales",
        default="10000,100000,1000000,10000000",
        help="comma-separated row counts, materialized mode only",
    )
    parser.add_argument(
        "--rows", type=int, default=2_000_000_000, help="row count, logical mode only"
    )
    parser.add_argument("--json-out", default="benchmarks/scale-results.json")
    parser.add_argument("--md-out", default="docs/scale-report.md")
    args = parser.parse_args()

    spark = build_spark()
    try:
        if args.mode == "materialized":
            scales = [int(s) for s in args.scales.split(",")]
            materialized = run_materialized(spark, scales)
            extrapolation = extrapolate_to_1tb(materialized)
            write_report(materialized, None, extrapolation, args.json_out, args.md_out)
        else:
            logical = run_logical(spark, args.rows)
            print(json.dumps(logical, indent=2))
            write_report(
                [],
                logical,
                None,
                args.json_out.replace(".json", "-logical.json"),
                args.md_out.replace(".md", "-logical.md"),
            )
    finally:
        spark.stop()

    print(
        f"\nwrote {args.json_out if args.mode == 'materialized' else args.json_out.replace('.json', '-logical.json')}"
    )


if __name__ == "__main__":
    main()
