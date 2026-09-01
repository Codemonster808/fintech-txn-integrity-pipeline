# Scale roadmap — from "extrapolation breaks before 1 TB" to "code we know scales"

`docs/scale-report.md` measured a real curve up to 10M rows and found the honest limit: a global shuffle-based dedup (`src/transformation/curate.py`) projects ~420 GB of shuffle volume at 1 TB, against 50 GB of free disk on this machine — the model breaks down before reaching 1 TB, not just slowly. This document covers the two structural changes that actually close that gap, both implemented and measured, not just proposed.

---

## Part 1 — the curation layer: bounded per-batch footprint

### Why more RAM doesn't fix it

In Spark's sort-based shuffle, shuffle write goes to local disk (`spark.local.dir`) by design — that's architectural, not a consequence of low memory. `disk_bytes_spilled` was **0** at every measured scale up to 10M rows (the shuffle data fit inside the 2 GB driver); there was no spill for more RAM to prevent. The 420 GB figure is shuffle *volume*, which more memory doesn't shrink.

### The fix: dedup against an external store, not against the whole dataset

`curate.py` dedupes by comparing the entire dataset against itself (`Window.partitionBy("idempotency_key")`) — a shuffle whose size grows with total volume. `src/transformation/curate_incremental.py` instead processes **bounded batches** and checks each batch's keys against a persistent DynamoDB table (`txn-curated-keys`), the same principle the real-time gate already uses (a conditional write, no shuffle, no need to see the rest of the data).

Design:
1. Intra-batch dedup — same window pattern as `curate.py`, scoped to one batch (bounded shuffle).
2. A Bloom filter (in-process, no dependency — `BloomFilter` in `curate_incremental.py`), rebuilt after every batch, cheaply rules out most definitely-new keys without a DynamoDB call.
3. `BatchGetItem` (100 keys/call) resolves whatever the filter couldn't rule out — run via Spark's `mapPartitions`, never a driver-side `.collect()` of the batch itself, so the DynamoDB calls genuinely distribute across partitions instead of serializing through the driver.
4. New rows go to S3; `BatchWriteItem` (25 items/call) registers their keys.

**Why two phases, not one atomic write:** `BatchWriteItem` doesn't support `ConditionExpression`, and `TransactWriteItems` fails the *entire* batch if any single condition fails — unusable when ~8% of a batch is expected to collide by design. This makes the curated layer at-least-once, not exactly-once — the same trade-off the outbox pattern already accepts elsewhere in this repo. The batch job is the only writer of `txn-curated-keys`; a duplicate row here is recoverable and does not touch the gate's real-time exactly-once guarantee on `txn-idempotency`.

### Two real bugs found while proving this, not just building it

1. **Fixed seeds meant every batch generated the same key space.** `synthetic_events_df` (from `scale_bench.py`) always produces `row_id` in `[0, n)` with fixed RNG seeds — called with the same `n` across batches, distinct batches would collide almost entirely by accident, not as a controlled test. Fixed by salting each batch's keys with its `batch_id`, isolating the *only* intentional overlap to a small, explicit replay of prior-batch keys.
2. **Lazy re-evaluation replayed the DynamoDB writes.** `mapPartitions` is lazy; calling `.write.parquet()`, `.count()`, and `.select().collect()` on the same DataFrame re-ran the whole transformation — including `BatchWriteItem` — up to three times. The first pass's writes became visible to the second pass's `BatchGetItem` reads, which then wrongly treated the first pass's own output as pre-existing duplicates. Caught by comparing per-partition debug counts across repeated runs and finding they grew between passes instead of matching. Fixed with `.cache()` (load-bearing, not an optimization) plus explicit `.unpersist()` so cached memory doesn't accumulate across the batch loop.

### The proof, and an honest correction to what was expected

Measured over 6 consecutive batches of 100,000 rows each (`benchmarks/incremental-results.json`):

| Batch | Elapsed | Rows/s | Cross-batch duplicates caught | New rows written |
|---|---|---|---|---|
| 0 | 35.7s | 2,804 | 0 (no prior batch to replay from) | 92,502 |
| 1 | 53.6s | 1,866 | 20 | 92,502 |
| 2 | 78.2s | 1,278 | 20 | 92,502 |
| 3 | 98.1s | 1,020 | 20 | 92,502 |
| 4 | 119.7s | 835 | 20 | 92,502 |
| 5 | 140.9s | 710 | 20 | 92,502 |

**What's flat, verified:** the actual work — rows deduped within a batch, cross-batch duplicates found, rows written — is identical batch to batch. That property (a batch's *correctness-relevant* output depends only on its own size, never on how many batches ran before it) holds, and is the property `curate.py`'s global shuffle genuinely lacks.

**What's NOT flat, found by measuring instead of assuming:** wall-clock time per batch grew from 35.7s to 140.9s over the run — roughly linear in batch number, not the flat curve the design predicted. Two candidate causes, not yet isolated by a controlled experiment:
1. The Bloom filter is sized once for the *whole run's* expected items (`n_batches × batch_rows`), so early batches see it far below its designed fill level — with fewer bits set, its real false-positive rate starts well under the 1% target and rises toward it as more keys are added. A higher false-positive rate means more `maybe_seen` keys per batch, each costing a real `BatchGetItem` call that a true negative would have skipped.
2. MiniStack's own DynamoDB implementation may not have AWS real DynamoDB's O(1) point-lookup regardless of table size — `txn-curated-keys` grows from ~92K to ~555K items over the run, and per-call latency growing with table size on the emulator side, independent of the Bloom filter, hasn't been ruled out.

Reported as measured, not adjusted to match the original prediction: **this design's per-batch resource footprint (shuffle, disk, rows processed) is genuinely bounded; its wall-clock time, on this measurement, was not** — and the fix depends on which of the two causes above actually dominates, which needs a follow-up experiment (e.g., a fixed-size Bloom filter reset per batch, isolating cause 1) before claiming either half of the fix.

### The 1 TB estimate — from the actually-measured rates, not an assumed per-call latency

An earlier version of this estimate assumed 2-5ms per DynamoDB call without measuring it. The 6-batch run above measured real rates instead — and they weren't constant (see above), so the honest estimate is a **range bounded by what was actually observed**, not a single number:

| | Value | Basis |
|---|---|---|
| Batches of 100K rows for 1 TB | 46,393 | 4,639,289,568 ÷ 100,000 |
| Best observed rate (batch 0, near-empty Bloom filter) | 2,804 rows/s → **~19.2 days** | measured |
| Worst observed rate (batch 5, still declining — not yet plateaued) | 710 rows/s → **~75.7 days** | measured |

The true steady-state rate is unknown from this run — batch 5 was still slower than batch 4, so the trend hadn't flattened out yet. The honest statement is "somewhere at or beyond 75.7 days at this table size and Bloom filter sizing, direction still declining," not a single point estimate. Isolating and fixing the cause (see above) is the next step before this range can be tightened.

**What moved, and what didn't:** the disk ceiling (a hard, physical limit on this machine — nothing makes 420 GB of shuffle fit in 50 GB) became a DynamoDB round-trip count with its own scaling behavior that turned out to be worse than assumed. That's a real trade, not a strictly better one — the disk wall is gone, but what replaced it needs its own investigation before it can be trusted at 1 TB.

**AWS pricing, if run against real DynamoDB, not MiniStack:**

| | Units | On-demand cost |
|---|---|---|
| Writes | 4,639M WCU | ~$5,800 |
| Reads, no Bloom filter | 4,639M RCU | ~$1,160 |
| Reads, with Bloom filter (~92% filtered) | ~371M RCU | ~$120 |

For a one-shot 1 TB load, that's real money to weigh against the alternative (a distributed Spark cluster). For the actual use case — an incremental daily pipeline — the per-run cost is a small fraction of this, since each run only processes new data, not the full historical volume.

---

## Part 2 — the gate: the larger bottleneck, addressed second on purpose

The curation-layer fix above doesn't touch the actual ceiling. Measured this session: **a single-threaded caller** gets ~273 requests/s from the gate (`benchmarks/results.json`, p50=3.66ms) — at 1 TB row counts, that's the number that dominated the original extrapolation, ~196 days vs. ~12h for the Spark side on the same volume. That figure was correct as documented (explicitly labeled "single-threaded"), but nobody had measured what the gate's real *concurrent* capacity actually was — `bench_gate_latency()` only ever ran one request at a time.

### What changed, and why in this order

1. **`bench_gate_saturation_curve()`** (`scripts/bench.py`) — the missing measurement. A concurrent client pool at 1/8/16/32/64 workers, reporting throughput and latency at each level. This has to come first: optimizing against a serial-caller number would have been optimizing against the wrong baseline.
2. **Metrics off the request path.** Every `/accept` call did 2-3 synchronous DynamoDB round-trips: one conditional `PutItem` (the real work) plus 1-2 `UpdateItem` calls just to increment counters. The counter accounting cost as much as the actual write. Fixed with `sync/atomic` counters and a periodic (2s) flush to `txn-gate-metrics` in a background goroutine — verified live: metrics correctly accumulate and land in DynamoDB on the flush tick, and a failed flush re-adds its delta instead of losing it.
3. **A bounded LRU cache of recently-accepted keys.** These synthetic retries arrive clustered in time (`data_gen.py` injects them 1-30s after the original). A cache hit answers 409 without touching DynamoDB at all — and it is safe by construction: a key only enters the cache *after* a successful `PutItem`, so a hit always means "this really was written before." A miss (including anything evicted) always falls through to the real conditional write; eviction can cost an extra DynamoDB call, never cause an incorrect accept.
4. **`POST /accept/batch`.** `BatchGetItem` (100/call) then `BatchWriteItem` (25/call) — round-trips per event drop from 1 to ~0.05. `BatchWriteItem` has no `ConditionExpression`, so this is two-phase, at-least-once, with an explicit race window (two concurrent batch requests containing the same brand-new key could both see it absent and both write it). `/accept` is unchanged and keeps the exact-one-winner guarantee — `tests/integration/test_chaos.py::test_concurrent_duplicate_requests_only_one_wins` still exercises it directly. Found and fixed one real bug building this: `BatchGetItem` rejects a request containing duplicate keys (real AWS behavior) — a batch with a within-request duplicate (a realistic case for a batch endpoint) failed with a 500 until the query keys were deduplicated separately from the per-event duplicate-marking logic.

### What this does and doesn't claim

This closes the accounting overhead and adds a genuine high-throughput path (`/accept/batch`) with an explicitly weaker, documented consistency guarantee. It does **not** claim a specific new events/s ceiling for `/accept` itself — see `benchmarks/gate-throughput-before.json` and `-after.json` for the actual measured concurrent numbers, captured under the same (uncontended) conditions for a fair before/after comparison. If a step's measured improvement didn't match what the design predicted, that's reported as measured, not adjusted to match the prediction.

---

## What's declared out of scope

- **Distributed Spark cluster** (EMR or similar) — the real fix for the *disk* ceiling this repo's single-machine `local[2]` Spark session can't avoid entirely; batching only pushes the wall further out, it doesn't remove Spark's own single-node limit for the intra-batch shuffle.
- **DynamoDB throttling/backoff on `UnprocessedKeys`/`UnprocessedItems`** — not observed on MiniStack at the volumes tested here; a real-AWS production version at sustained high throughput would need this. Stated as a known simplification in both `curate_incremental.py` and `main.go`, not silently absent.
- **Horizontal gate scaling** (N instances behind a load balancer) — the gate is already stateless with shared state in DynamoDB, so this needs no code change, only infrastructure this repo doesn't stand up. Documented, not demonstrated, because there's no way to verify it honestly on one machine.
