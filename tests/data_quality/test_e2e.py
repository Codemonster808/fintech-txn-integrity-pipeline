"""
End-to-end quality test: the full producer -> SNS -> SQS -> gate ->
validator -> S3 -> Step Functions -> PySpark -> DuckDB flow, against
live MiniStack, scored on the 5 standard data-quality dimensions rather
than a single pass/fail assertion.

Requires: docker compose up -d, scripts/bootstrap.py already run,
the gate binary built (cd src/ingestion/gate && go build ./...).
"""

import ast
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from utils import aws  # noqa: E402
from utils.quality import Dimension, QualityReport  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
GATE_URL = "http://localhost:8080"
GATE_BIN = REPO_ROOT / "src" / "ingestion" / "gate" / "gate"
N_EVENTS = 1000
RETRY_RATE = 0.08


@pytest.fixture(scope="module")
def gate_process():
    if not GATE_BIN.exists():
        pytest.skip(
            "gate binary not built — run `cd src/ingestion/gate && go build ./...` "
            f"first ({GATE_BIN})"
        )
    proc = subprocess.Popen([str(GATE_BIN)], env={"GIN_MODE": "release"})
    for _ in range(20):
        try:
            if requests.get(f"{GATE_URL}/health", timeout=1).status_code == 200:
                break
        except requests.ConnectionError:
            time.sleep(0.25)
    else:
        proc.terminate()
        pytest.fail("gate did not become healthy in time")
    yield proc
    proc.terminate()
    proc.wait(timeout=5)


def _clear_bucket_prefix(s3, bucket: str, prefix: str) -> None:
    objs = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    for o in objs:
        s3.delete_object(Bucket=bucket, Key=o["Key"])


def test_full_pipeline_quality(gate_process):
    run_id = uuid.uuid4().hex[:8]
    data_path = REPO_ROOT / "data" / f"e2e_events_{run_id}.jsonl"

    # --- generate ground truth ---
    gen = subprocess.run(
        [
            sys.executable,
            "src/ingestion/data_gen.py",
            "--out",
            str(data_path),
            "--count",
            str(N_EVENTS),
            "--retry-rate",
            str(RETRY_RATE),
            "--seed",
            "123",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert gen.returncode == 0, gen.stderr
    events = [json.loads(line) for line in data_path.read_text().splitlines()]
    unique_keys = {e["idempotency_key"] for e in events}

    s3 = aws.client("s3")
    _clear_bucket_prefix(s3, "txn-raw", "valid/")
    _clear_bucket_prefix(s3, "txn-quarantine", "invalid/")

    # --- publish ---
    pub = subprocess.run(
        [sys.executable, "src/ingestion/publisher.py", "--in", str(data_path)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert pub.returncode == 0, pub.stderr

    # --- consume: gate + validator ---
    t0 = time.perf_counter()
    consume = subprocess.run(
        [sys.executable, "src/ingestion/consumer.py", "--idle-timeout", "8"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )
    consume_seconds = time.perf_counter() - t0
    assert consume.returncode == 0, consume.stderr
    consume_stats = json.loads(consume.stdout)

    # --- Step Functions daily job (preflight -> curate -> postflight) ---
    t0 = time.perf_counter()
    sm = subprocess.run(
        [sys.executable, "src/orchestration/statemachine.py"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=180,
    )
    daily_job_seconds = time.perf_counter() - t0
    assert sm.returncode == 0, sm.stderr
    sm_result = json.loads(sm.stdout[sm.stdout.index("{") :])

    # --- second curate run, for consistency check ---
    curate2 = subprocess.run(
        [sys.executable, "src/transformation/curate.py"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert curate2.returncode == 0, curate2.stderr

    # --- build the quality report ---
    report = QualityReport(pipeline="fintech-txn-integrity-pipeline")

    n_raw_valid = s3.list_objects_v2(Bucket="txn-raw", Prefix="valid/").get("KeyCount", 0)
    report.check(
        Dimension.COMPLETENESS,
        "no_settled_txn_lost",
        measured=n_raw_valid,
        threshold=len(unique_keys),
        detail=(
            f"S3 raw valid objects ({n_raw_valid}) vs unique idempotency keys "
            f"generated ({len(unique_keys)})"
        ),
    )

    measured_dup_rate = consume_stats["duplicate_rejected"] / (
        consume_stats["processed"] + consume_stats["duplicate_rejected"]
    )
    rate_delta = abs(measured_dup_rate - RETRY_RATE)
    report.check(
        Dimension.CORRECTNESS,
        "dedup_rate_matches_injected",
        measured=round(rate_delta, 4),
        threshold=0.02,
        higher_is_better=False,
        detail=f"measured dedup rate {measured_dup_rate:.4f} vs injected {RETRY_RATE}",
    )
    report.check(
        Dimension.CORRECTNESS,
        "zero_processing_errors",
        measured=consume_stats["errors"],
        threshold=0,
        higher_is_better=False,
        detail="consumer error count",
    )

    curate_stats = sm_result.get("curate_stats", {})
    curate2_line = next(
        line for line in curate2.stdout.splitlines() if line.startswith("curated: ")
    )
    curate2_stats = ast.literal_eval(curate2_line[len("curated: ") :])
    report.check(
        Dimension.CONSISTENCY,
        "curate_reprocess_same_row_count",
        measured=1.0 if curate2_stats.get("rows_out") == curate_stats.get("rows_out") else 0.0,
        threshold=1.0,
        detail=(
            f"first run rows_out={curate_stats.get('rows_out')}, "
            f"second run rows_out={curate2_stats.get('rows_out')}"
        ),
    )

    n_quarantine = s3.list_objects_v2(Bucket="txn-quarantine", Prefix="invalid/").get("KeyCount", 0)
    report.check(
        Dimension.VALIDITY,
        "no_valid_events_misrouted_to_quarantine",
        measured=n_quarantine,
        threshold=0,
        higher_is_better=False,
        detail="all synthetic events use schema_version=1, so quarantine should be empty",
    )

    report.check(
        Dimension.TIMELINESS,
        "daily_job_under_sla",
        measured=round(daily_job_seconds, 1),
        threshold=120.0,
        higher_is_better=False,
        detail="preflight + curate + postflight wall time",
    )
    report.check(
        # ~35ms/event is what sequential HTTP (gate) + S3 PutObject
        # (validator) actually costs at this volume — measured at 35.4s for
        # 1000 events on a first run; 45s threshold leaves real headroom
        # without being loose enough to hide a regression.
        Dimension.TIMELINESS,
        "consumer_throughput_under_sla",
        measured=round(consume_seconds, 1),
        threshold=45.0,
        higher_is_better=False,
        detail=f"{N_EVENTS} events through gate+validator",
    )

    report.to_json(str(REPO_ROOT / "benchmarks" / "quality-report.json"))
    report.to_markdown(str(REPO_ROOT / "docs" / "quality-report.md"))

    data_path.unlink(missing_ok=True)

    report.assert_all_passed()
