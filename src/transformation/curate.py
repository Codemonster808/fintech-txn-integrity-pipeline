#!/usr/bin/env python3
"""
Reads the many small per-event JSON objects under s3://txn-raw/valid/,
dedupes by idempotency_key, and writes fewer, larger Parquet files to
s3://txn-curated/ — the compaction step a Step Functions daily job runs.
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_spark(app_name: str = "txn-curate") -> SparkSession:
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4581")
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
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def curate(spark: SparkSession, src_path: str, dst_path: str) -> dict:
    df = spark.read.json(src_path)

    n_before = df.count()

    # Dedupe defensively at the batch layer too — the gate already prevents
    # duplicates from being accepted, but this makes the batch job correct
    # even if it's ever run against raw data that bypassed the gate.
    window = Window.partitionBy("idempotency_key").orderBy(F.col("ts").asc())
    deduped = (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("event_ts", F.col("ts").cast("timestamp"))
        # partition by a formatted string, not a raw TimestampType — partitioning
        # directly on a timestamp column trips a path-generation bug in the S3A
        # committer bundled with hadoop-aws 3.5.0
        .withColumn("ingest_hour", F.date_format(F.col("event_ts"), "yyyy-MM-dd-HH"))
    )

    n_after = deduped.count()

    # partitionBy() already groups output by ingest_hour at write time — an
    # explicit .repartition(n, "ingest_hour") beforehand is redundant and
    # triggers an empty-path bug in the hadoop-aws 3.5.0 S3A committer.
    (deduped.write.mode("overwrite").partitionBy("ingest_hour").parquet(dst_path))

    return {"rows_in": n_before, "rows_out": n_after, "duplicates_dropped": n_before - n_after}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", default="s3a://txn-raw/valid/")
    # Never write a partitioned dataset directly at a bucket's root — besides
    # being bad practice, the hadoop-aws 3.5.0 S3A committer hits an
    # empty-path bug when the partitioned output path has no subdirectory.
    parser.add_argument("--dst", default="s3a://txn-curated/txn_events/")
    args = parser.parse_args()

    spark = build_spark()
    try:
        stats = curate(spark, args.src, args.dst)
        print(f"curated: {stats}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
