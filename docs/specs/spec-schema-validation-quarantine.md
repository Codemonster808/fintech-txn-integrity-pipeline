# Spec: schema validation and quarantine

## Objetivo de negocio

Un cambio de schema o un evento malformado no debe desaparecer. El ledger
solo recibe filas válidas; lo inválido se aparca con razón explícita para
replay cuando el registry o el productor se corrijan.

## Fuentes de entrada

- Eventos ya aceptados por el gate (`POST /accept` → 200).
- Registry `s3://txn-raw/_schema_registry/current_version.json`
  (`{"current_version": 1}`), creado por `ensure_schema_registry()` si falta.

## Transformaciones

`src/models/validator.py:validate_event` / `handler`:

- Campos requeridos: `txn_id`, `idempotency_key`, `account_id`,
  `amount_cents`, `currency`, `schema_version`, `ts`.
- `schema_version` debe igualar `current_version`.
- `amount_cents` debe ser `int` y `> 0`.

## Salida esperada

- Válido → `s3://txn-raw/valid/{idempotency_key}.json`.
- Inválido → `s3://txn-quarantine/invalid/{idempotency_key}.json` más
  `_quarantine_reason`. Nunca drop silencioso.

## Casos borde

- `schema_version` 99 vs registry 1 → cuarentena con mismatch en el reason.
- `amount_cents` negativo o no entero → cuarentena.
- Campo requerido ausente → cuarentena listando los missing.

## Criterios de aceptación

- `features/schema-validation.feature`
- `tests/unit/test_validator.py`
