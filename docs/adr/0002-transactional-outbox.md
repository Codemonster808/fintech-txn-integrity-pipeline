# ADR 0002 — Transactional outbox vs dual-write vs CDC

## Contexto

Hay que notificar "curation completed" a SNS sin perder el evento si el
broker falla después de persistir el job.

## Decisión

Escribir el status del job y un row `PENDING` en `txn-outbox` en el
mismo `TransactWriteItems`. Un proceso aparte (`outbox_publisher.py`)
publica y marca `PUBLISHED`.

## Alternativas consideradas

- **Dual-write** (PutItem luego `sns.publish`): si el publish falla, el
  hecho quedó grabado y nadie se entera. Es el bug que este patrón evita.
- **CDC/Debezium sobre DynamoDB streams**: correcto a escala AWS real;
  MiniStack no ofrece un stream fiable aquí, y el portfolio demostraría
  infra que no corre. El outbox cabe en una Lambda + un poller.

## Consecuencias

At-least-once hacia SNS (el publisher puede reintentar). Consumidores
del topic de curación deben ser idempotentes. El demo no auto-flushea
el outbox para que el patrón sea observable en el RUNBOOK.
