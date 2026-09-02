# CLAUDE.md — fintech-txn-integrity-pipeline

Operative constitution for working in this repo. Not a tour of the architecture —
see `docs/architecture.md` and `docs/adr/` for that.

## 1. Domain context — what "correct data" means here

This is an exactly-once transaction ingestion pipeline for payment platforms. The
one thing that must never happen: a retried transaction settles twice.

- A duplicate `idempotency_key` sent to `POST /accept` gets **409**, and leaves
  **no row** in `txn-idempotency` beyond the original — a failed conditional
  `PutItem` leaves no trace. Verify this with
  `tests/integration/test_idempotency.py` and `tests/integration/test_chaos.py`,
  never by inspection alone.
- `POST /accept/batch` intentionally does **not** give the same guarantee — it
  trades atomicity for throughput (`BatchGetItem`/`BatchWriteItem`, no
  `ConditionExpression`) and has a documented race window where two concurrent
  batches carrying the same brand-new key can both report "accepted". This is a
  stated trade-off, not a bug — do not "fix" it by adding a fake lock; if the
  atomic guarantee matters, use `/accept`.
- An event whose `schema_version` doesn't match the registry (or that fails
  required-field/`amount_cents > 0` checks) is **quarantined with a
  `_quarantine_reason`**, never silently dropped and never written to
  `txn-raw`.
- Re-running `src/transformation/curate.py` against the same raw input must
  produce the same `rows_out` — `tests/data_quality/test_e2e.py` checks this
  directly (`curate_reprocess_same_row_count`).

## 2. Exact commands

Every recipe in the Makefile runs under
`set -a && source ./env.sh --quiet && set +a` first — always `source env.sh`
(don't execute it) before running anything by hand outside `make`.

```bash
export AWS_ENDPOINT_URL=http://localhost:4581   # this machine; see docker-compose.yml
export MINISTACK_PORT=4581
docker compose up -d
make check-env          # scripts/check_env.py — MiniStack reachability
make demo               # 200 events, learn/iterate — builds the gate, boots MiniStack,
                         # bootstraps resources, runs the full pipeline once
make demo-full          # 100k events — regenerates the published README metrics (~1h)
make test                # pytest tests/ -v --ignore=tests/data_quality (unit + integration)
make e2e                 # pytest tests/data_quality -v -s (full pipeline, quality-report.json)
make build-gate           # cd src/ingestion/gate && go build ./...
make bench                # scripts/bench.py -> benchmarks/results.json
make bench-gate-concurrent  # real concurrent gate capacity -> benchmarks/gate-throughput.json
make curate                # src/transformation/curate.py, one-off
make curate-incremental    # bounded-batch alternative, opt-in (docs/scale-roadmap.md)
make scale-bench            # measured dedup/curate curve, opt-in, ~10-15 min
make scale-bench-logical    # billions of in-flight rows, no disk I/O, opt-in
make outbox                 # src/orchestration/outbox_publisher.py, opt-in
make query                  # DuckDB against s3://txn-curated via sql/daily_settlement.sql
make inspect                 # scripts/aws_inspect.py all
```

There is no `make build-worker`/`make catalog`/`make eval` equivalent in this
repo — this pipeline has no ECS worker, warehouse catalog job, or LLM eval
harness; don't invent flags that aren't in the Makefile.

Dependencies: `requirements.in` (direct runtime deps) and `requirements-dev.in`
(lint/type/security tooling, constrained against `requirements.txt` so the two
never disagree) are the source of truth — never hand-edit `requirements.txt`
or `requirements-dev.txt`, they're generated:
```bash
.venv/bin/pip-compile requirements.in --output-file requirements.txt
.venv/bin/pip-compile requirements-dev.in --output-file requirements-dev.txt
```
This is also what makes Dependabot's pip PRs resolvable instead of hand-editing
one pinned line into a conflict with another.

## 3. Naming conventions

**S3 buckets**: `txn-raw` (valid events, `valid/` prefix; schema registry at
`_schema_registry/current_version.json`), `txn-quarantine` (`invalid/` prefix,
each object carries `_quarantine_reason`), `txn-curated` (compacted Parquet,
partitioned by `ingest_hour`).

**DynamoDB tables**: `txn-idempotency` (hash key `idempotency_key`, the single
source of truth for exactly-once), `txn-gate-metrics` (hash key `metric_id`,
atomic counters flushed every 2s by the gate), `txn-curation-jobs` (hash key
`job_id`, but a job has 2+ status rows — keyed as
**`job_id = "{job_id}#{status}"`** so `started` and `completed` don't collide),
`txn-outbox` (hash key `event_id`, `{job_id}#CurationCompleted`, status
`PENDING`/`PUBLISHED` — the transactional-outbox table), `txn-curated-keys`
(hash key `idempotency_key`, the persistent cross-batch store
`curate_incremental.py` checks against, mirroring what the gate does in
real time).

**SNS/SQS**: topic `txn-events` -> queues `txn-validation-queue` +
`txn-audit-queue` (-> DLQ `txn-audit-dlq`); topic `txn-curation-events` ->
queue `txn-curation-events-queue` (the outbox's publish target).

**Lambda functions**: `txn-preflight`, `txn-record-status` — source in
`src/orchestration/lambdas/`, deployed by `src/orchestration/statemachine.py`
from `asl/preflight.json` and `asl/postflight.json`.

**Commits/branches**: no enforced convention beyond descriptive messages;
follow the existing git log style in this repo rather than inventing one.

## 4. Schema and data rules

- `schema_version` on every event is validated against the registry object at
  `s3://txn-raw/_schema_registry/current_version.json` (`src/models/validator.py`).
  A version mismatch, a missing required field, or a non-positive
  `amount_cents` all route to `txn-quarantine` with `_quarantine_reason` set —
  **never** dropped silently, and never allowed into `txn-raw`.
- The idempotency gate (`src/ingestion/gate/main.go`) is the single source of
  truth for exactly-once: one conditional `PutItem` per key, everything else
  downstream is at-least-once by design (SNS/SQS redelivery, the outbox
  pattern, `curate.py`'s defensive re-dedup).
- The transactional outbox (`src/orchestration/lambdas/record_status.py` +
  `src/orchestration/outbox_publisher.py`) commits the job-status row and the
  `PENDING` outbox event in one `transact_write_items` call — never add a
  direct `sns.publish()` inside a Lambda handler; that reintroduces the exact
  failure mode (a lost event after a committed write) the pattern exists to
  prevent.
- Re-curating the same raw input must be byte-for-byte consistent in row
  count — a change that breaks `curate_reprocess_same_row_count` in
  `tests/data_quality/test_e2e.py` is a regression, not a rounding error.

## 5. What NOT to touch without confirming

- `.env` — never commit it (gitignored on purpose; `.env.example` is the
  template).
- Buckets/tables/queues — never delete or truncate against a live MiniStack
  without asking first; `scripts/bootstrap.py` is idempotent but destructive
  cleanup is not its job.
- `scripts/iam_setup.py` — creates real IAM roles/policies; safe to re-run
  (idempotent) but don't change the least-privilege grants in `iam/*.json`
  without understanding what `docs/RUNBOOK.md` §5's
  `simulate-principal-policy` exercise depends on.
- `AWS_ENDPOINT_URL` / `MINISTACK_PORT` — this machine runs MiniStack on
  `4581`, not the `4566` default, because other repos in this portfolio run
  their own MiniStack concurrently on other ports. Don't "fix" this back to
  4566.
- `LLM_PROVIDER` and `VECTOR_BACKEND` env vars exist in `env.sh`/`common`-
  derived `utils/llm` and `utils/vectors` purely because this repo shares a
  template with `agentic-claims-copilot` — this pipeline does not call an LLM
  or a vector store anywhere in its actual flow. Don't wire them in without
  a real reason; they cost money in other repos, not this one.

## 6. Where specs and features live

Read before implementing, not after:
- `docs/specs/` — one spec per pipeline feature (objective, inputs,
  transformations, expected output/SLA, edge cases, acceptance criteria).
- `docs/adr/` — design decisions and the alternatives that were rejected,
  with the real trade-off stated.
- `features/*.feature` + `features/steps/` — pytest-bdd scenarios, each tied
  to a spec's acceptance criteria. Run via `make test` (wired into the normal
  pytest collection, not a separate command).

## 7. PII and synthetic data

All data in this repo is synthetic and deterministic by seed
(`--seed 42` in `data_gen.py`, `--seed 123` in `tests/data_quality/test_e2e.py`).
Do not introduce real transaction data, real account identifiers, or real
customer PII anywhere in this repo — fixtures, tests, or benchmarks. Do not
log full event payloads in production-style code paths (amounts and account
ids are fine in this synthetic context, but don't build the habit of dumping
raw payloads to logs).

Notebooks are not part of this repo and are not expected to appear — if one
ever does, it is exploratory only and must never be imported into or run as
part of the pipeline (`src/`, `scripts/`, `asl/`, or `make` targets).
