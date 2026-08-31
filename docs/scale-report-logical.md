# Scale benchmark — fintech-txn-integrity-pipeline

1 TB of real events (237-byte events, measured) = **4,639,289,568 rows**. This machine has 50 GB free disk — a literal 1 TB dedup shuffle does not fit here. What follows is measured up to the largest scale that does fit, extrapolated from there with explicit assumptions.

## Logical mode (in-flight, nothing written to disk)

- Requested rows: 1,000,000,000
- Processed rows: 999,998,027
- Logical bytes: 220.7 GB (equivalent 21.6% of 1 TB)
- Elapsed: 341.882s (2,924,976.0 rows/s)
- **Nothing was written to S3** — this is generated and consumed in-flight, not a materialized-throughput claim.
