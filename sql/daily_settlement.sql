SELECT
    ingest_hour,
    COUNT(*) AS n_transactions,
    COUNT(DISTINCT idempotency_key) AS n_unique_keys,
    SUM(amount_cents) / 100.0 AS total_amount,
    currency
FROM txn_curated
GROUP BY ingest_hour, currency
ORDER BY ingest_hour;
