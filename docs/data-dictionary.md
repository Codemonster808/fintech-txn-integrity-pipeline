# Data dictionary — fintech-txn-integrity-pipeline

Every dataset below is produced by exactly one component in this repo. "Lineage"
names that producer so a reader can go straight to the source instead of
guessing.

## S3 objects

### `s3://txn-raw/valid/{idempotency_key}.json`

Raw, schema-valid transaction events, one JSON object per key.

| Field | Type | Notes |
|---|---|---|
| `txn_id` | string (UUID) | unique per physical send attempt, including retries |
| `idempotency_key` | string (UUID) | the dedup key; shared by an original event and its retries |
| `account_id` | string | `acc_NNNNNN`, synthetic |
| `amount_cents` | int | must be > 0 (enforced by the validator) |
| `currency` | string | one of `USD`/`EUR`/`MXN`/`COP` |
| `schema_version` | int | must match the registry's `current_version` at write time |
| `ts` | ISO-8601 string | event timestamp |

**Lineage**: written by `src/models/validator.py:handler()`, called from
`src/ingestion/consumer.py` after the Go gate accepts an event.

### `s3://txn-raw/_schema_registry/current_version.json`

`{"current_version": 1}` — the schema-version registry consulted by every
validation. Lineage: `ensure_schema_registry()` in `src/models/validator.py`
(created on first use if absent).

### `s3://txn-quarantine/invalid/{idempotency_key}.json`

Same shape as `txn-raw/valid/`, plus a `_quarantine_reason` string field
(e.g. `"schema_version mismatch: got 99, expected 1"`,
`"missing fields: ['amount_cents']"`, `"amount_cents must be a positive
integer"`). Never dropped, never silently discarded. Lineage: same
`validator.py:handler()`, the failure branch.

### `s3://txn-curated/txn_events/ingest_hour=YYYY-MM-DD-HH/*.parquet`

Deduplicated, compacted Parquet, partitioned by `ingest_hour`
(`yyyy-MM-dd-HH`, derived from `ts`). Grain: one row per unique
`idempotency_key`, first-seen-by-`ts` wins (`Window.partitionBy
("idempotency_key").orderBy("ts")`, keep `_rn == 1`). Columns: `txn_id`,
`idempotency_key`, `account_id`, `amount_cents`, `currency`,
`schema_version`, `event_ts`, `ingest_hour`, `ts`. Lineage:
`src/transformation/curate.py:curate()`, run daily via
`src/orchestration/statemachine.py`'s subprocess step. Authoritative DDL
for the equivalent Redshift table lives in `sql/redshift/schema.sql`
(table `txn_curated`, `DISTKEY(account_id)`, `SORTKEY(ingest_hour)`).

### `s3://txn-curated/incremental/batch=NNNNN/*.parquet`

Same row shape, produced by the bounded-batch alternative path. Lineage:
`src/transformation/curate_incremental.py:process_batch()` — opt-in, not
part of `make demo`/`make e2e`.

## DynamoDB tables

| Table | Hash key | Written by | Purpose |
|---|---|---|---|
| `txn-idempotency` | `idempotency_key` (S) | `src/ingestion/gate/main.go` (`acceptHandler`) | source of truth for exactly-once; a successful conditional `PutItem` here is the only signal a key has been accepted |
| `txn-gate-metrics` | `metric_id` (S, always `"counters"`) | `src/ingestion/gate/main.go` (`flushMetrics`, every 2s) | `total_requests`, `duplicate_rejections` atomic counters — read by `GET /metrics/dedup` in `src/serving/api.py` |
| `txn-curation-jobs` | `job_id` (S) — actually stored as **`"{job_id}#{status}"`** so a job's `started` and `completed` rows don't collide | `src/orchestration/lambdas/record_status.py` | one row per (job, status) pair; carries `raw_object_count` on `started`, `curated_row_count`/`duplicates_dropped` on `completed` |
| `txn-outbox` | `event_id` (S, `"{job_id}#CurationCompleted"`) | `src/orchestration/lambdas/record_status.py` (transactionally with the jobs row) | transactional-outbox events; `status` is `PENDING` until `src/orchestration/outbox_publisher.py` publishes to SNS and flips it to `PUBLISHED` |
| `txn-curated-keys` | `idempotency_key` (S) | `src/transformation/curate_incremental.py` | persistent cross-batch dedup store for the incremental curation path — mirrors the gate's real-time table, one level up in the batch layer |

## SQS / SNS

| Resource | Type | Notes |
|---|---|---|
| `txn-events` | SNS topic | producer side; `src/ingestion/publisher.py` publishes here |
| `txn-validation-queue` | SQS queue | subscribed to `txn-events`, raw delivery; consumed by `src/ingestion/consumer.py` |
| `txn-audit-queue` | SQS queue | second subscriber to `txn-events`, redrive to `txn-audit-dlq` |
| `txn-audit-dlq` | SQS queue | dead-letter target, `maxReceiveCount=3` |
| `txn-curation-events` | SNS topic | outbox's publish target |
| `txn-curation-events-queue` | SQS queue | subscribed to `txn-curation-events` |

## Serving layer (FastAPI, `src/serving/api.py`)

| Endpoint | Backing data |
|---|---|
| `GET /txn/{idempotency_key}` | reads `txn-raw/valid/{key}.json` or `txn-quarantine/invalid/{key}.json` |
| `GET /metrics/dedup` | reads `txn-gate-metrics` counters |
| `GET /metrics/sla` | queries the DuckDB view over `s3://txn-curated/txn_events/**/*.parquet` (`src/utils/warehouse.py`) |

## Reference-only (not executed in this dev stack)

`sql/redshift/schema.sql` — the authoritative DDL for a real Redshift
deployment of `txn_curated` (not run against DuckDB; DuckDB's
`read_parquet()` in `src/utils/warehouse.py` is the local stand-in with the
same S3-Parquet access pattern as a Redshift `COPY`/Spectrum query).
`sql/daily_settlement.sql` — the query `make query` runs against that DuckDB
view (transaction counts/totals per `ingest_hour` and `currency`).

## Deliberately absent

No `notebooks/` and no `dbt/` in this repo — there is no exploratory notebook
work and no dbt usage anywhere in the pipeline. See `CLAUDE.md` §7 for the
rule if one ever gets added: notebooks never feed the production pipeline
directly.
