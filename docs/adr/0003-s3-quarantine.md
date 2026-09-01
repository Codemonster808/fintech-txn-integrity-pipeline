# ADR 0003 — Cuarentena en S3 vs drop vs rechazo en el borde

## Contexto

Eventos con schema viejo o `amount_cents` inválido. Perderlos es peor
que un load fallido: no hay replay.

## Decisión

Tras el gate (ya accepted), el validator escribe inválidos a
`txn-quarantine` con `_quarantine_reason`. Válidos van a `txn-raw`.

## Alternativas consideradas

- **Drop silencioso**: irrecuperable; un bump de schema vacía el ledger
  sin alarma.
- **Rechazo 4xx en el gate Go**: mezcla idempotencia con schema; el gate
  debe permanecer un hop de milisegundos. El schema vive en Python junto
  al registry en S3.

## Consecuencias

El gate puede aceptar una key que luego cuarentena — no hay asentamiento
en curated. Replay = arreglar registry/productor y re-inyectar desde
quarantine (proceso operativo, no automático en el demo).
