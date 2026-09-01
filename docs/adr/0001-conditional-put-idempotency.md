# ADR 0001 — Idempotencia con PutItem condicional

## Contexto

Los productores reintentan. SQS es at-least-once. El ledger no puede
asentar dos veces la misma `idempotency_key`.

## Decisión

`POST /accept` hace un único `PutItem` con
`ConditionExpression: attribute_not_exists(idempotency_key)` sobre
`txn-idempotency`. 200 si gana, 409 si pierde. Un LRU en proceso solo
acelera 409 conocidos; un miss siempre cae al Put real.

## Alternativas consideradas

- **Check-then-write** (`GetItem` + `PutItem`): race real — dos accepts
  concurrentes de una key nueva pueden ambos ver "ausente".
- **Lock distribuido** (DDB lock / Redis): más latencia y failure modes
  (lock leak) para un recurso que ya es una escritura atómica.
- **SQS FIFO + MessageGroupId**: dedup de cola, no de negocio; un retry
  con nuevo `MessageDeduplicationId` igual asienta dos veces.

## Consecuencias

Exactly-once en `/accept` (probado: 20 concurrentes → 1×200 + 19×409).
`/accept/batch` **no** usa ConditionExpression (BatchWrite no lo
soporta); la ventana de carrera está documentada y testeada, no oculta.
