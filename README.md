# fintech-txn-integrity-pipeline

Exactly-once transaction ingestion pipeline for payment platforms, built to run entirely on LocalStack.

## Pitch Card

**Problem** — Payment platforms lose money and customer trust when retried transactions settle twice. Producer retries, at-least-once queues, and batch reprocessing all create duplicate settlements that reach the ledger before anyone notices.

**Solution** — An exactly-once ingestion pipeline: an idempotency-key gate at the edge (Go/Gin) backed by DynamoDB conditional writes, schema-versioned validation, Parquet compaction, and a daily Spark/Redshift serving layer — fully reproducible on LocalStack.

**Impact** — 0 duplicate settlements ever land in the ledger, exactly-once holds under a 20-way concurrent race on the same key (1 accepted, 19 rejected — see `tests/test_chaos.py`), 4.1 ms p95 gate latency (200 requests), duplicate rate measured at 0.10 against a 500-event run with a 10% injected retry rate — matching the injected rate exactly. ~$1.9M/yr modeled avoided double-settlement (see `docs/impact-model.md` for assumptions).

**Stack** — Python 3 · PySpark · FastAPI · Go/Gin · AWS (S3, SNS, SQS, Lambda, DynamoDB, Step Functions, Redshift) via LocalStack

---

## Architecture

```
synthetic txn producer (deliberate retries/duplicates)
  → SNS topic `txn-events`
  → SQS fan-out: validation queue + audit queue (+ DLQ)
  → Go/Gin idempotency gate: hash(idempotency_key) → DynamoDB conditional PutItem
  → Lambda validator: schema version check → quarantine bucket on failure
  → S3 raw (JSON, partitioned by ingest hour)
  → Step Functions daily job: PySpark curate + Parquet compaction → S3 curated
  → Redshift COPY from S3 curated
  → FastAPI: /txn/{id} status, /metrics/dedup, /metrics/sla
```

See `docs/architecture.md` for the diagram.

## Why Go here

The idempotency gate is the hot path: short request, stateless, high QPS, one responsibility (conditional write + 200/409 response). A compiled Go binary avoids Python cold-start latency at this specific chokepoint.

**Honesty note:** this repo is production-grade Python; Go is used as a bounded, single-purpose worker at the ingestion edge — not evidence of Go platform seniority.

## Measured in this repo

| Metric | Value | How it's measured |
|---|---|---|
| Gate p95 latency (200 requests) | **4.11 ms** | `make bench` → `benchmarks/results.json` |
| Gate mean latency | **3.69 ms** | `make bench` |
| Duplicate rate (500 events, 10% injected retries) | **0.10** — matches injected rate exactly | `python3 src/bench.py` after a clean `data_gen.py` + gate replay |
| Concurrent race on one key (20 simultaneous requests) | **1 accepted, 19 rejected**, every time | `pytest tests/test_chaos.py::test_concurrent_duplicate_requests_only_one_wins` |
| Spark curate job | 191/191 rows preserved, 0 lost | `python3 src/curate.py` |
| Test suite | **10/10 passing, re-runnable** (no hardcoded keys — see below) | `pytest tests/ -v`, run twice in a row |

> Numbers above are from actual runs against MiniStack + the real Go gate on this machine, not projected. `make bench` regenerates them.

## Modeled business impact (synthetic data — assumptions documented)

| Assumption | Source | Modeled outcome |
|---|---|---|
| 8M txn/month, 0.4% duplicate settlement rate absent dedup | Public fintech ops benchmark (cited in `docs/impact-model.md`) | ~$1.9M/yr avoided double-settlement |

> These figures are a **model** built on the assumptions above, not measured production results.

## Emulated vs. real

| Component | Dev (this repo) | Production would use | Fidelity |
|---|---|---|---|
| S3 / SQS / SNS / Lambda / DynamoDB | [MiniStack](https://ministack.org) (free, MIT, no account) | AWS | High |
| Step Functions | MiniStack (full ASL interpreter) | AWS | Medium-High |
| AWS CLI v2 | Real `aws` CLI against MiniStack (`AWS_ENDPOINT_URL`, no `--endpoint-url` flag needed) — verified against all 7 services above | AWS CLI v2 | High — see `docs/RUNBOOK.md` §2 for the exact commands |
| IAM | MiniStack accepts real roles/policies (`create-role`, `put-role-policy`, `assume-role`) and `iam simulate-principal-policy` evaluates them correctly — but does **not enforce** them on live S3/DynamoDB/Lambda calls (verified: a role with an explicit `Deny *` could still call `s3 ls`) | AWS IAM | Medium — real policy authoring/validation, no live enforcement; `docs/RUNBOOK.md` §5 has the `simulate-principal-policy` exercise |
| Redshift | **DuckDB**, reading Parquet directly from S3 (`httpfs`) — same access pattern as Redshift `COPY`/Spectrum | Redshift Serverless | Medium — no MPP distribution; real `DISTKEY`/`SORTKEY` DDL shipped in `sql/redshift/` for reference |

All AWS access goes through `boto3` with `endpoint_url` set via `AWS_ENDPOINT_URL` (see `common/aws.py`) — swapping MiniStack for `moto` or real AWS is a one-line change, not a rewrite.

## Three non-tutorial challenges

1. **Real exactly-once**: idempotency key + DynamoDB `ConditionExpression: attribute_not_exists`, including the edge case of a retry arriving after a partial commit.
2. **Schema evolution**: `schema_version` embedded per event, registry in S3, explicit policy for new/removed fields, quarantine bucket with targeted replay.
3. **Small-file compaction**: measured trade-off between file size and PUT/GET request count, with before/after Redshift query time.

## Demo (3 minutes)

```bash
source env.sh
make demo        # 200 events — learn / iterate (see docs/RUNBOOK.md)
make demo-full   # 100k events — regenerates README-scale metrics (~1h)
pytest tests/test_idempotency.py
make query
```

## Learn by running

See [`docs/RUNBOOK.md`](docs/RUNBOOK.md) (step-by-step + what to inspect + what to break). Build from scratch: [`docs/BUILD_GUIDE.md`](docs/BUILD_GUIDE.md).

## What this is NOT

Not a "Kafka word count" with AWS bolted on. Not a happy-path ETL — failure modes (retries, schema drift, late compaction) are injected deliberately and tested.

## Build it yourself

See [`docs/BUILD_GUIDE.md`](docs/BUILD_GUIDE.md) for a step-by-step build guide, written so it's followable without prior AWS/Spark experience.
