-- Reference DDL for an actual Redshift deployment (not executed against DuckDB dev).
-- DuckDB's read_parquet() in src/common/warehouse.py is the local stand-in;
-- this file documents what production distribution/sort keys would look like.

CREATE TABLE IF NOT EXISTS txn_curated (
    txn_id           VARCHAR(36)   NOT NULL,
    idempotency_key  VARCHAR(36)   NOT NULL,
    account_id       VARCHAR(32)   NOT NULL,
    amount_cents     BIGINT        NOT NULL,
    currency         CHAR(3)       NOT NULL,
    schema_version   SMALLINT      NOT NULL,
    ingest_hour      TIMESTAMP     NOT NULL,
    ts               TIMESTAMP     NOT NULL
)
DISTSTYLE KEY
DISTKEY (account_id)
SORTKEY (ingest_hour);

-- Loaded via:
-- COPY txn_curated FROM 's3://txn-curated/'
-- IAM_ROLE 'arn:aws:iam::<account>:role/redshift-copy-role'
-- FORMAT AS PARQUET;
