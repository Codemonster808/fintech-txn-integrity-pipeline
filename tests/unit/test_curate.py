"""Window-dedupe in transformation/curate.py.

The batch job keeps the earliest row per idempotency_key even if raw
data bypassed the Go gate. Same Spark-on-a-tiny-DataFrame pattern as
delivery-eta-mesh's tests/unit/test_replay.py. Spec: docs/specs/ and
src/transformation/curate.py (duplicates_dropped).
"""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "transformation"))

from curate import build_spark, curate  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = build_spark("test-curate")
    yield s
    s.stop()


def test_curate_keeps_earliest_row_per_idempotency_key(spark, tmp_path):
    src = tmp_path / "valid"
    src.mkdir()
    rows = [
        {"idempotency_key": "k1", "ts": "2026-01-01T00:02:00Z", "txn_id": "later"},
        {"idempotency_key": "k1", "ts": "2026-01-01T00:01:00Z", "txn_id": "earliest"},
        {"idempotency_key": "k2", "ts": "2026-01-01T00:03:00Z", "txn_id": "unique"},
    ]
    (src / "events.json").write_text("\n".join(json.dumps(r) for r in rows))
    dst = tmp_path / "curated"

    stats = curate(spark, str(src), str(dst))

    assert stats == {"rows_in": 3, "rows_out": 2, "duplicates_dropped": 1}
    out = spark.read.parquet(str(dst))
    kept = {r["txn_id"] for r in out.collect()}
    assert kept == {"earliest", "unique"}
