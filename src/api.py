#!/usr/bin/env python3
"""FastAPI serving layer: transaction status, dedup metrics, SLA metrics."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, HTTPException  # noqa: E402

from common import aws, warehouse  # noqa: E402

app = FastAPI(title="fintech-txn-integrity-pipeline")


def _warehouse_con():
    con = warehouse.connect()
    try:
        warehouse.read_parquet(con, "s3://txn-curated/txn_events/**/*.parquet", "txn_curated")
    except Exception:
        con.execute("CREATE OR REPLACE VIEW txn_curated AS SELECT NULL AS txn_id WHERE FALSE")
    return con


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/txn/{idempotency_key}")
def get_txn(idempotency_key: str):
    s3 = aws.client("s3")
    for bucket, prefix, status in [
        ("txn-raw", "valid/", "settled"),
        ("txn-quarantine", "invalid/", "quarantined"),
    ]:
        key = f"{prefix}{idempotency_key}.json"
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            import json
            return {"status": status, **json.loads(obj["Body"].read())}
        except s3.exceptions.NoSuchKey:
            continue
        except Exception:
            continue
    raise HTTPException(status_code=404, detail="transaction not found")


@app.get("/metrics/dedup")
def dedup_metrics():
    """
    Reads the atomic counters the Go gate maintains in txn-gate-metrics
    (UpdateItem/ADD on every request, accepted or rejected). A duplicate
    rejection never creates a row in txn-idempotency — a failed
    conditional PutItem leaves no trace there — so counting rejections
    from that table would always read zero. That's why this is a
    separate counter, bumped at the point of rejection, not inferred
    after the fact.
    """
    ddb = aws.client("dynamodb")
    try:
        item = ddb.get_item(
            TableName="txn-gate-metrics",
            Key={"metric_id": {"S": "counters"}},
        ).get("Item", {})
    except Exception:
        item = {}

    total = int(item.get("total_requests", {}).get("N", 0))
    duplicates = int(item.get("duplicate_rejections", {}).get("N", 0))
    return {
        "total_requests": total,
        "duplicate_rejections": duplicates,
        "duplicate_rate": round(duplicates / total, 4) if total else 0.0,
    }


@app.get("/metrics/sla")
def sla_metrics():
    con = _warehouse_con()
    row = con.execute("""
        SELECT COUNT(*) AS rows_curated,
               COUNT(DISTINCT ingest_hour) AS hours_covered,
               MIN(ingest_hour) AS earliest_hour,
               MAX(ingest_hour) AS latest_hour
        FROM txn_curated
    """).fetchone()
    return {
        "rows_curated": row[0],
        "hours_covered": row[1],
        "earliest_hour": row[2],
        "latest_hour": row[3],
    }
