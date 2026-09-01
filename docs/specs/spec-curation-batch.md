# Spec: curation batch (compact JSON → Parquet)

## Objetivo de negocio

El hot path no puede compactar. Durante el día aterrizan muchos JSON
chicos en `txn-raw`. Un job diario los fusiona a Parquet consultable
(warehouse / DuckDB) sin perder filas.

## Fuentes de entrada

- `s3://txn-raw/valid/` (JSON por key).
- Orquestación: `src/orchestration/statemachine.py` — preflight Lambda →
  driver Spark → postflight Lambda. SLA medido del job: ≤120 s en demo.

## Transformaciones

`src/transformation/curate.py:curate`:

- Lee JSON, dedupe por `idempotency_key` (first-seen por `ts` gana).
- Particiona por `ingest_hour` (`yyyy-MM-dd-HH` derivado de `ts`).
- Escribe Parquet coalescido.

Camino opt-in: `curate_incremental.py` (batches acotados + tabla
`txn-curated-keys`). No forma parte de `make demo`.

## Salida esperada

- `s3://txn-curated/txn_events/ingest_hour=.../*.parquet`
- Grain: una fila por `idempotency_key`.
- Reproceso del mismo raw → mismo `rows_out`.

## Casos borde

- Raw que bypaseó el gate: el batch sigue siendo correcto por el window
  de dedupe.
- Job `started` y `completed` no colisionan: DDB key `{job_id}#{status}`.

## Criterios de aceptación

- `tests/data_quality/test_e2e.py` (`curate_reprocess_same_row_count`)
