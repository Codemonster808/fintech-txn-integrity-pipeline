# Spec: transactional outbox (curation completed)

## Objetivo de negocio

El hecho de negocio "el job de curación terminó" y el evento SNS que lo
anuncia no pueden divergir. Un `sns.publish()` después de un `PutItem`
puede perder el evento si el publish falla.

## Fuentes de entrada

- Postflight: `src/orchestration/lambdas/record_status.py` con
  `status=completed`, counts de curate.

## Transformaciones

- Mismo `TransactWriteItems`: fila en `txn-curation-jobs` + fila
  `PENDING` en `txn-outbox` (`event_id = "{job_id}#CurationCompleted"`).
- Publisher aparte: `src/orchestration/outbox_publisher.py` publica a
  SNS `txn-curation-events` y marca `PUBLISHED`. No corre dentro de
  `make demo` a propósito (el RUNBOOK deja PENDING visible).

## Salida esperada

- O ambas filas existen, o ninguna.
- Cola `txn-curation-events-queue` recibe el mensaje solo tras el publisher.

## Casos borde

- Fallo de SNS: el row PENDING permanece para retry; el job completed
  no se "deshace".
- Lambda corre *dentro* de MiniStack: el endpoint interno es
  `127.0.0.1:4566`, no el puerto host 4581.

## Criterios de aceptación

- Comentarios y transact en `record_status.py`; ejercicio en `docs/RUNBOOK.md`.
