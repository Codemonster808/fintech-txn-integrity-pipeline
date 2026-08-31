# Scale benchmark — fintech-txn-integrity-pipeline

1 TB of real events (237-byte events, measured) = **4,639,289,568 rows**. This machine has 50 GB free disk — a literal 1 TB dedup shuffle does not fit here. What follows is measured up to the largest scale that does fit, extrapolated from there with explicit assumptions.

## Materialized curve (real Parquet, real shuffle)

| Rows | Status | Write input (s) | Dedup+write output (s) | Rows/s (dedup phase) | Shuffle spill (disk) |
|---|---|---|---|---|---|
| 10,000 | OK | 10.806 | 3.987 | 2,508.2 | 0.0 MB |
| 100,000 | OK | 2.25 | 7.808 | 12,806.9 | 0.0 MB |
| 1,000,000 | OK | 24.385 | 10.329 | 96,818.3 | 0.0 MB |
| 10,000,000 | OK | 370.472 | 90.284 | 110,761.3 | 0.0 MB |

## Extrapolation to 1 TB

Based on the 10,000,000-row measured run (110,761.3 rows/s, dedup phase):

- Extrapolated time for 1 TB through the dedup/curate path: **11.6 hours**
- Projected shuffle volume at 1 TB: 419.5 GB (free disk: 50 GB, driver memory: 2 GB)
- **Model breaks down: True** — projected shuffle volume at 1 TB (419.5 GB) is 210x the 2 GB driver memory and exceeds free disk (50 GB) — at every measured scale (up to 10M rows) disk_bytes_spilled was 0 because shuffle data fit in driver memory; at 1 TB it would not, forcing spill, and there isn't enough disk for that spill either. This is not achievable on this machine at any speed — it would require a distributed shuffle across multiple nodes, not just more time on one.

Assumptions: linear scaling of the largest measured rows/second past the measured range; same single-machine hardware, no contention from other workloads; shuffle spill scales linearly with row count (no algorithmic change).

### The real bottleneck isn't Spark

Measured gate latency (p50=3.66ms, single-threaded): **273.2 events/s**. At that rate, 1 TB through the gate alone: **196.5 days** — vs. 11.6 hours for the Spark dedup phase on the same row count. The gate is a single synchronous HTTP hop per event — at 1 TB row counts this is the real bottleneck, orders of magnitude below Spark's dedup throughput. Scaling this pipeline to TB volumes means batching the idempotency check, not tuning Spark.
