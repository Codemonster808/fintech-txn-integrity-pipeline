# Architecture

```mermaid
flowchart LR
    P[Synthetic txn producer\nwith injected retries] -->|publish| SNS[SNS: txn-events]
    SNS --> SQSV[SQS: validation queue]
    SNS --> SQSA[SQS: audit queue]
    SQSV --> GATE[Go/Gin idempotency gate]
    GATE -->|conditional PutItem| DDB[(DynamoDB\nidempotency table)]
    GATE -->|accepted| LAMBDA[Lambda: schema validator]
    LAMBDA -->|invalid| QUAR[(S3: quarantine)]
    LAMBDA -->|valid| S3RAW[(S3: raw, partitioned by hour)]
    SF[Step Functions: daily job] --> SPARK[PySpark: curate + compact]
    S3RAW --> SPARK
    SPARK --> S3CUR[(S3: curated Parquet)]
    S3CUR -->|COPY| RS[(Redshift)]
    RS --> API[FastAPI: /txn/id, /metrics/dedup, /metrics/sla]
    SQSA --> DLQ[(SQS DLQ: audit failures)]
```

## Data flow notes

- The idempotency gate is the only synchronous, latency-sensitive hop. Everything downstream is asynchronous.
- The daily Step Functions job is what performs compaction — small files land in `S3RAW` all day, and get merged into fewer, larger Parquet files in `S3CUR` once per day.
- The quarantine bucket exists so a schema change never silently drops data — it's parked for replay once the schema registry entry is fixed.
