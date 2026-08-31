# Quality report — fintech-txn-integrity-pipeline

Generated: 2026-08-31T23:17:15.327175+00:00

**Overall score: 100%** (7/7 checks passed)

| Dimension | Score |
|---|---|
| completeness | 100% |
| correctness | 100% |
| consistency | 100% |
| validity | 100% |
| timeliness | 100% |

## Checks

| Dimension | Check | Measured | Threshold | Status | Detail |
|---|---|---|---|---|---|
| completeness | no_settled_txn_lost | 920 | 920 | PASS | S3 raw valid objects (920) vs unique idempotency keys generated (920) |
| correctness | dedup_rate_matches_injected | 0.0 | 0.02 | PASS | measured dedup rate 0.0800 vs injected 0.08 |
| correctness | zero_processing_errors | 0 | 0 | PASS | consumer error count |
| consistency | curate_reprocess_same_row_count | 1.0 | 1.0 | PASS | first run rows_out=920, second run rows_out=920 |
| validity | no_valid_events_misrouted_to_quarantine | 0 | 0 | PASS | all synthetic events use schema_version=1, so quarantine should be empty |
| timeliness | daily_job_under_sla | 53.1 | 120.0 | PASS | preflight + curate + postflight wall time |
| timeliness | consumer_throughput_under_sla | 30.0 | 45.0 | PASS | 1000 events through gate+validator |
