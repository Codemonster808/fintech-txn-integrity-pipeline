# Spec: idempotency gate

## Objetivo de negocio

Ninguna transacción reintentada debe liquidarse dos veces. Los productores
reintentan ante fallos de ack ambiguos; el gate es el único punto síncrono
del pipeline y su trabajo es garantizar exactly-once en la aceptación de una
`idempotency_key`, sin bloquear el resto del sistema (todo lo posterior es
async y at-least-once).

## Fuentes de entrada

- `POST /accept` — un evento de transacción individual (`txn_id`,
  `idempotency_key`, `account_id`, `amount_cents`, `currency`,
  `schema_version`, `ts`), enviado por `src/ingestion/consumer.py` después de
  leer de `txn-validation-queue`.
- `POST /accept/batch` — lista de eventos, para ingesta de alto volumen donde
  at-least-once es aceptable.

## Transformaciones

- `/accept`: un único `PutItem` condicional a DynamoDB
  (`ConditionExpression: attribute_not_exists(idempotency_key)`) sobre
  `txn-idempotency`. Antes de tocar DynamoDB, un LRU cache en memoria
  (`recentKeysCacheCapacity = 50_000`) puede responder 409 sin round-trip si
  la key ya fue vista por este proceso — un cache miss siempre cae al
  `PutItem` real, nunca al revés, así que el cache solo puede acelerar el
  camino de duplicado, jamás aceptar por error.
- `/accept/batch`: dos fases — `BatchGetItem` (100 keys/call) para ver qué
  existe, luego `BatchWriteItem` (25 items/call) para el resto. Ninguna de
  las dos soporta `ConditionExpression`, así que esto es lectura-luego-
  escritura, no una escritura atómica.
- Métricas (`total_requests`, `duplicate_rejections`) se acumulan en
  contadores atómicos en memoria y se vuelcan a `txn-gate-metrics` cada 2s
  (`metricsFlushInterval`), nunca en el camino síncrono del request.

## Salida esperada

- `200 {"status": "accepted", "txn_id": ...}` — primera vez que se ve la key.
- `409 {"status": "duplicate", "idempotency_key": ...}` — key ya existente.
- Para `/accept/batch`: `200 {"results": [{"idempotency_key", "status"}, ...]}`,
  un resultado por evento del batch, incluyendo duplicados dentro del mismo
  batch.
- SLA de latencia medido: p50 2.87ms / p95 según `make bench` (single-
  threaded); capacidad concurrente real medida vía
  `bench_gate_throughput_concurrent()` (`scripts/bench.py`), no inferida del
  número serial.

## Casos borde

- Reintento tardío (segundos/minutos después del original): sigue
  rechazado — no hay ventana de expiración en `txn-idempotency`.
- Race de 20 requests concurrentes con la misma key nueva contra `/accept`:
  exactamente 1 acepta, 19 rechazan, siempre — la atomicidad del
  `ConditionExpression` lo garantiza.
- La misma race contra `/accept/batch`: **puede** haber más de un
  "accepted" — ventana de carrera documentada y aceptada, no un bug. La
  propiedad que se exige en ese caso es que ningún request falle y que el
  estado final en DynamoDB sea correcto (una fila), no que gane exactamente
  uno.
- Un batch con la misma key repetida dentro del mismo request: `BatchGetItem`
  rechaza claves duplicadas en la consulta (comportamiento real de AWS, no
  un artefacto de MiniStack) — el gate dedupe las claves consultadas por
  separado del marcado de duplicados por evento.

## Criterios de aceptación

- `features/idempotency.feature` — reenvío de la misma `idempotency_key` →
  409 sin fila nueva; 20 peticiones concurrentes → exactamente 1×200 y
  19×409; reintento tardío (1s después) sigue rechazado.
- Verificado también por `tests/integration/test_idempotency.py` y
  `tests/integration/test_chaos.py` (incluye el camino `/accept/batch` y su
  ventana de carrera documentada).
