# Build Guide — fintech-txn-integrity-pipeline

This guide assumes no prior AWS or Spark experience. Every step has a command and an expected result you can check. Estimated total: ~22 hours across 2-3 weeks of evenings.

## Glossary (read once, refer back as needed)

- **MiniStack**: a free, MIT-licensed program that pretends to be AWS on your own laptop (S3, SQS, DynamoDB, Lambda, Step Functions, etc.) — no account, no API key, ~30MB RAM idle. This repo's free AWS substitute.
- **DuckDB**: an embedded, columnar SQL database — no server to run. Stands in for Redshift here: it reads Parquet files directly from S3 (via MiniStack), the same access pattern as a real Redshift `COPY`/Spectrum query.
- **Idempotency key**: a unique ID attached to a request so that processing it twice has the same effect as processing it once.
- **DLQ (dead-letter queue)**: a "failed messages" holding queue — nothing gets silently dropped.
- **Parquet**: a compressed, column-oriented file format used for analytics data.
- **Compaction**: merging many small files into fewer large ones, because small files are slow and expensive to query.

## 0. Before you start (30 min)

Install and verify (one-time machine setup — see `portfolio-overview/setup_env.sh` if Docker/Go aren't installed yet):

```bash
docker --version        # need 24+ (native Docker Engine, not Docker Desktop)
python3 --version       # need 3.12+
go version               # need 1.21+
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Get MiniStack working:

```bash
docker compose up -d
curl http://localhost:4566/_health   # expected: {"services": {...}} with no errors
```

If the health check fails, check `docker compose logs ministack` — the most common cause is Docker Desktop's context still being active (`docker context use default` fixes it).

## 1. Get the environment running (1 h) → checkpoint: `make check-env`

`docker-compose.yml` already declares a single `ministack` service exposing port 4566 with S3, SNS, SQS, Lambda, DynamoDB, and Step Functions. Redshift has no free local equivalent — that's what `common/warehouse.py` (DuckDB) is for; nothing to configure here.

```bash
docker compose up -d
python3 scripts/bootstrap.py   # creates the SNS topic, SQS queues+DLQ, DynamoDB table, S3 buckets
make check-env   # should print: "OK: services reachable"
```

**Troubleshooting**
- Port 4566 already in use → another MiniStack/LocalStack instance is running; `docker ps` and stop it.
- `check-env` times out → MiniStack takes a couple seconds to boot (much faster than LocalStack); wait and retry.
- boto3 calls hang or hit real AWS → confirm `AWS_ENDPOINT_URL=http://localhost:4566` is set (see `.env` / `common/aws.py`).

## 2. Generate synthetic data (1-2 h) → checkpoint: `make check-data`

A transaction event looks like:

```json
{"txn_id": "uuid", "idempotency_key": "uuid", "account_id": "acc_123", "amount_cents": 4599, "currency": "USD", "schema_version": 1, "ts": "2026-01-01T12:00:00Z"}
```

Write `src/data_gen.py` that produces 100k of these, with 8% deliberately re-sent (same `idempotency_key`, different `txn_id` timing) to simulate producer retries.

```bash
python3 src/data_gen.py --out data/events.jsonl --count 100000 --retry-rate 0.08
make check-data   # should print: "OK: 100000 events, 8.0% retries, schema valid"
```

## 3. Build the ingestion path (3-4 h) → checkpoint: `make check-ingest`

Build one piece at a time — do not write the whole path before testing the first hop.

1. Create the SNS topic and SQS queues (+ DLQ) via `awslocal`. Verify: `awslocal sqs list-queues`.
2. Write the Go/Gin idempotency gate (`src/gate/main.go`): receives an event, does a conditional `PutItem` on DynamoDB keyed by `idempotency_key`. Verify: send the same event twice, confirm the second gets HTTP 409.
3. Write the Lambda validator: checks `schema_version` against a registry file in S3; on mismatch, writes to the quarantine bucket instead of raw.

```bash
make check-ingest   # replays 1000 events through the full path, asserts 0 duplicates land in S3 raw
```

**Troubleshooting**
- Gate returns 500 on every request → check the DynamoDB table's key schema matches `idempotency_key` exactly.
- Everything gets quarantined → your test data's `schema_version` doesn't match the registry; check both are `1`.

## 4. Build the compute/transform layer (4-6 h) → checkpoint: `make check-transform`

Write the PySpark job (`src/curate.py`) that reads `S3RAW`, dedupes, and writes compacted Parquet to `S3CUR`, partitioned by day.

```bash
make check-transform   # asserts file count in S3CUR is <10% of file count in S3RAW for the same day
```

## 5. Build the serving layer + API (2-3 h) → checkpoint: `make check-api`

Point DuckDB's `httpfs` extension at MiniStack's S3 endpoint and query `S3CUR` directly (`SELECT * FROM read_parquet('s3://txn-curated/**/*.parquet')`) — this is the local equivalent of a Redshift `COPY`. The real `DISTKEY`/`SORTKEY` DDL for an actual Redshift deployment lives in `sql/redshift/schema.sql` for reference. Write the FastAPI app (`src/api.py`) with `/txn/{id}`, `/metrics/dedup`, `/metrics/sla`.

```bash
uvicorn src.api:app --reload
curl localhost:8000/metrics/dedup   # expected: {"duplicate_rate": 0.0, "total_events": 100000}
```

## 6. Add the failure paths (3-4 h) → checkpoint: `make check-chaos`

This is the part that earns the interview. Inject:
- A burst of retries arriving out of order.
- A schema version bump mid-stream (some events v1, some v2).
- A DynamoDB throttling error (simulate with a LocalStack fault injection or a manual retry-storm).

```bash
make check-chaos   # all three scenarios must resolve to 0 duplicate settlements and 0 silently dropped events
```

## 7. Measure it (2 h) → checkpoint: `make bench`

```bash
make bench   # writes benchmarks/results.json with p95 latency, dedup rate, file counts before/after compaction
```

Copy these numbers into the README's "Measured in this repo" table.

## 8. Write the impact model (1 h)

Fill in real sources in `docs/impact-model.md`. Do not publish a dollar figure without a citation.

## 9. Ship the README (1 h)

Fill the Pitch Card, both metric tables, and the language-boundary honesty statement. Confirm `make demo` works from a clean clone.

## Troubleshooting index

| Symptom | Likely cause | Fix |
|---|---|---|
| `awslocal` command not found | pip package not installed | `pip install awscli-local` |
| Gate always returns 409 | idempotency table not cleared between test runs | `make check-env` resets it |
| Spark job hangs | too many partitions for local mode | reduce `spark.sql.shuffle.partitions` to 4 in dev config |

## Total estimated effort: ~22 hours (2-3 weeks of evenings)
