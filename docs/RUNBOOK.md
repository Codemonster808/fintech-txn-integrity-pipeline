# Runbook — aprender el pipeline (P1)

Complementa `docs/BUILD_GUIDE.md` (cómo se **construyó**). Esto es cómo se **corre** y se entiende, paso a paso.

**Tiempo:** 20–30 min. **Escala:** 200 eventos (`make demo`). `make demo-full` son 100k (~1 h) — solo para regenerar métricas del README.

---

## 0. Una sola vez por terminal

```bash
cd /home/lesaint/Documentos/life_plans/fintech-txn-integrity-pipeline
source env.sh
```

Debes ver `AWS_ENDPOINT_URL = http://localhost:4566`. Si no lo ves, los errores más adelante (`QueueDoesNotExist`, `ResourceNotFoundException`) **no** significan que falte el recurso: significan que boto3 está pegándole a AWS real (o a ningún emulador).

```bash
docker compose up -d
curl -s http://localhost:4566/health
make check-env
python3 scripts/bootstrap.py
python3 scripts/aws_inspect.py all     # vacío todavía: buckets 0, colas 0
```

Un repo a la vez. `docker compose down` antes de abrir otro (RAM).

---

## 1. Flujo paso a paso

### 1.1 Generar eventos (con retries deliberados)

```bash
python3 src/ingestion/data_gen.py --out data/events.jsonl --count 200 --retry-rate 0.08 --seed 42
wc -l data/events.jsonl
python3 -c "import json; ks=[json.loads(l)['idempotency_key'] for l in open('data/events.jsonl')]; print(len(ks), 'lines', len(set(ks)), 'unique keys')"
```

**Qué entender:** 200 líneas ≠ 200 keys. El 8% son reintentos del productor: misma `idempotency_key`, distinto timing. Eso es lo que el gate Go tiene que rechazar.

### 1.2 Publicar a SNS (todavía no hay nada en S3)

```bash
cd src/ingestion/gate && go build ./... && cd ../..
python3 src/ingestion/publisher.py --in data/events.jsonl
python3 scripts/aws_inspect.py sqs
python3 scripts/aws_inspect.py s3
```

**Qué inspeccionar:** `txn-validation-queue` visible > 0. Buckets `txn-raw` / `txn-quarantine` siguen en 0. El mensaje está en el bus, nadie lo ha consumido.

### 1.3 Encender el gate y consumir (aquí aparece S3)

El gate **no** puede quedar colgado en background. En dos terminales:

**Terminal A** (deja el gate vivo mientras aprendes):

```bash
source env.sh
GIN_MODE=release src/ingestion/gate/gate
# /health en :8080
```

**Terminal B:**

```bash
source env.sh
curl -s http://localhost:8080/health
python3 src/ingestion/consumer.py --idle-timeout 10
python3 scripts/aws_inspect.py s3
python3 scripts/aws_inspect.py ddb
python3 scripts/aws_inspect.py sqs
```

**Qué inspeccionar:**
- S3 `txn-raw/valid/` — eventos aceptados
- DynamoDB `txn-idempotency` — una fila por key
- `txn-gate-metrics` — contador de duplicados (no se infiere de DDB: un PutItem condicional fallido no deja rastro)
- SQS visible ≈ 0

Cuando termines de explorar, `Ctrl+C` en Terminal A.
Atajo que sí limpia el proceso: `make demo` (usa `scripts/run_with_bg.sh`).

### 1.4 Step Functions + curación Spark

```bash
python3 src/orchestration/statemachine.py
python3 scripts/aws_inspect.py sfn
python3 src/transformation/curate.py          # Parquet en txn-curated
make query                     # DuckDB (stand-in de Redshift)
```

### 1.5 Outbox: publicar el evento de finalización

`src/orchestration/statemachine.py` ya dejó un evento `CurationCompleted` en `PENDING` dentro de `txn-outbox` (lo escribe `record_status.py` en la misma transacción DynamoDB que la fila de status — ver su docstring). Nadie lo publicó a SNS todavía:

```bash
aws dynamodb scan --table-name txn-outbox --query 'Items[*].[event_id.S,status.S]' --output text
make outbox                    # o: python3 src/orchestration/outbox_publisher.py
aws dynamodb scan --table-name txn-outbox --query 'Items[*].[event_id.S,status.S]' --output text
```

**Qué entender:** `make outbox` no corre solo dentro de `make demo` — a propósito, para que veas el evento quedarse en `PENDING` hasta que alguien lo publique. Esa separación (escribir el hecho de negocio + el evento pendiente atómicamente, publicar después, en otro proceso) es el patrón *transactional outbox*: si `record_status.py` publicara a SNS directo después del `put_item`, un fallo justo ahí perdería el evento en silencio aunque el job quedara marcado `completed`. Ver el ejercicio de la sección 5 para romperlo a propósito.

---

## 2. Explorar con AWS CLI

`scripts/aws_inspect.py` es un resumen curado. El AWS CLI real es lo que usarías en un trabajo — mismos comandos, mismo endpoint, sin ningún script del repo de por medio (`aws` respeta `AWS_ENDPOINT_URL` que exporta `env.sh`, no hace falta `--endpoint-url`).

```bash
# S3 — mismo estado que "aws_inspect.py s3", sin el resumen
aws s3 ls
aws s3 ls s3://txn-raw/valid/ --recursive
aws s3 ls s3://txn-quarantine/ --recursive

# SQS — visibility timeout, redrive policy a la DLQ, mensajes in-flight
QUEUE_URL=$(aws sqs get-queue-url --queue-name txn-validation-queue --query QueueUrl --output text)
aws sqs get-queue-attributes --queue-url "$QUEUE_URL" --attribute-names All

# DynamoDB — item real, no el resumen compactado de aws_inspect.py
aws dynamodb scan --table-name txn-idempotency --max-items 5
aws dynamodb describe-table --table-name txn-gate-metrics --query 'Table.ItemCount'

# Outbox — eventos pendientes de publicar y ya publicados
aws dynamodb scan --table-name txn-outbox --query 'Items[*].[event_id.S,status.S]' --output text

# SNS — suscripciones del topic (quién recibe cada evento)
TOPIC_ARN=$(aws sns list-topics --query "Topics[?contains(TopicArn,'txn-events')].TopicArn" --output text)
aws sns list-subscriptions-by-topic --topic-arn "$TOPIC_ARN"

# Step Functions — el historial de transiciones, estado por estado
SM_ARN=$(aws stepfunctions list-state-machines --query "stateMachines[0].stateMachineArn" --output text)
EXEC_ARN=$(aws stepfunctions list-executions --state-machine-arn "$SM_ARN" --max-results 1 --query "executions[0].executionArn" --output text)
aws stepfunctions get-execution-history --execution-arn "$EXEC_ARN"

# Lambda — estado real de las funciones que despliega src/orchestration/statemachine.py
aws lambda get-function --function-name txn-preflight --query 'Configuration.[State,Runtime,LastModified]'
aws lambda invoke --function-name txn-preflight --payload '{}' /tmp/out.json && cat /tmp/out.json

# IAM — roles reales con políticas de mínimo privilegio (scripts/iam_setup.py, iam/*.json)
aws iam list-roles --query 'Roles[?starts_with(RoleName,`txn-`)].RoleName'
aws iam get-role-policy --role-name txn-preflight-role --policy-name least-privilege
```

**Qué mirar que `aws_inspect.py` no te muestra:** `RedrivePolicy` en la cola (a qué DLQ va después de cuántos intentos), `get-execution-history` completo (cada `Choice`/`Retry`/`Catch` real, no solo el resumen de status), y la respuesta cruda de `lambda invoke` (payload de retorno, no solo "Active").

---

## 3. Romper a propósito

### Duplicado → 409

Con el gate en Terminal A:

```bash
# toma la primera key del archivo
python3 - <<'PY'
import json, requests
e = json.loads(open("data/events.jsonl").readline())
print("key", e["idempotency_key"])
print("1st", requests.post("http://localhost:8080/accept", json=e).status_code)
print("2nd", requests.post("http://localhost:8080/accept", json=e).status_code)
PY
```

Esperado: `200` luego `409`. El segundo no escribe otro objeto en S3.

### Sin `env.sh`

Abre una terminal **nueva** (sin `source env.sh`) y corre `python3 src/ingestion/publisher.py --in data/events.jsonl`.
Esperado: timeout, credenciales, o `QueueDoesNotExist`. Ese es el Problema 1 del handoff.

---

## 4. Errores que vas a ver

| Error | Qué significa de verdad |
|---|---|
| `QueueDoesNotExist` / `ResourceNotFoundException` | Casi siempre: **no cargaste `env.sh`**. Segundo: no corriste `bootstrap.py`. |
| `bind: address already in use` :8080 | Gate o worker Java de P4 quedó vivo. `lsof -ti:8080 \| xargs -r kill` |
| `Connection refused` :8080 | Consumer corrió sin el gate. Enciende Terminal A. |
| `Can not create a Path from an empty string` | Spark/S3A: no escribas Parquet en la raíz del bucket; usa un subpath. |

---

## 5. Ejercicios

**1. ¿Cuántos mensajes hay realmente in-flight, y por qué eso importa?**

Con el gate y el consumer corriendo, corta el consumer a mitad de un batch (`Ctrl+C` en Terminal B mientras procesa) y corre `aws sqs get-queue-attributes --queue-url $QUEUE_URL --attribute-names All` antes de que expire el visibility timeout.

<details><summary>Verificar</summary>

`ApproximateNumberOfMessagesNotVisible` > 0: esos mensajes están "prestados" al consumer que murió, invisibles para otros, hasta que pase `VisibilityTimeout` (30s aquí). Si el consumer no hace `delete_message` a tiempo, SQS los vuelve a entregar — así es como un consumer que crashea no pierde datos, pero si el bug es del consumer (no del mensaje), tras `maxReceiveCount` (ver `RedrivePolicy`) el mensaje termina en `txn-audit-dlq`, no reintenta para siempre.
</details>

**2. Publica directo al topic SNS y mira el fan-out sin pasar por `publisher.py`**

`aws sns publish --topic-arn $TOPIC_ARN --message '{"idempotency_key": "manual-test-1", "amount": 1}'`, luego revisa las dos colas suscritas.

<details><summary>Verificar</summary>

El mensaje aparece en **ambas** `txn-validation-queue` y `txn-audit-queue` (`aws sqs receive-message --queue-url ...`) — es el mismo fan-out que hace `publisher.py`, pero ahora lo disparaste tú directamente contra el topic, sin ninguna línea de código del repo de por medio. Así se ve una arquitectura pub/sub real: el productor no sabe ni le importa cuántos consumidores hay.
</details>

**3. Reconstruye qué pasó en una ejecución de Step Functions solo con el CLI**

Corre `python3 src/orchestration/statemachine.py`, luego usa **solo** `aws stepfunctions list-executions` y `get-execution-history` (sección 2) — sin mirar `src/orchestration/statemachine.py` ni `asl/*.json`.

<details><summary>Verificar</summary>

`get-execution-history` te da, en orden: `ExecutionStarted` → por cada estado, `TaskStateEntered`/`TaskScheduled`/`TaskSucceeded` (o `TaskFailed` si algo truena) → `ExecutionSucceeded`. El campo `stateEnteredEventDetails.name` te dice qué Choice/Task se ejecutó — puedes reconstruir el flujo completo de la ASL sin leer el JSON, que es justo lo que harías depurando en AWS real donde no tienes el código a la mano.
</details>

**4. Encuentra el objeto S3 sin usar `aws_inspect.py`**

Después de `make demo`, usa solo `aws s3api list-objects-v2 --bucket txn-raw --prefix valid/` para encontrar un objeto, y `aws s3api get-object --bucket txn-raw --key <esa-key> /tmp/evento.json` para bajarlo.

<details><summary>Verificar</summary>

`list-objects-v2` (no `s3 ls`) te da `ETag`, `Size`, `LastModified` por objeto — metadata que `aws_inspect.py` no imprime. Comparar el `Size` contra `wc -c` del archivo bajado confirma que no hubo truncamiento en la subida.
</details>

**5. Quita un permiso de verdad, valídalo con `simulate-principal-policy`, arréglalo**

`src/orchestration/statemachine.py` ya crea 3 roles reales con políticas de mínimo privilegio (`scripts/iam_setup.py`, `iam/*.json`) — pero MiniStack **no aplica enforcement** en las llamadas reales (un rol con `Deny *` explícito igual podría hacer `s3 ls`; verificado). Lo que sí evalúa políticas de verdad es `iam simulate-principal-policy` — así es como validarías una política *antes* de desplegarla en un trabajo real.

```bash
# 1. Confirma el estado actual: allowed
aws iam simulate-principal-policy --policy-source-arn arn:aws:iam::000000000000:role/txn-preflight-role \
  --action-names s3:ListBucket --resource-arns "arn:aws:s3:::txn-raw"

# 2. Quita el permiso — reemplaza el statement por uno que no lo incluya
cat > /tmp/reduced-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "Placeholder", "Effect": "Allow", "Action": "s3:GetBucketLocation", "Resource": "arn:aws:s3:::txn-raw"}
  ]
}
EOF
aws iam put-role-policy --role-name txn-preflight-role --policy-name least-privilege --policy-document file:///tmp/reduced-policy.json

# 3. Simula de nuevo — debe salir implicitDeny
aws iam simulate-principal-policy --policy-source-arn arn:aws:iam::000000000000:role/txn-preflight-role \
  --action-names s3:ListBucket --resource-arns "arn:aws:s3:::txn-raw"

# 4. Restaura la política real del repo y confirma allowed otra vez
aws iam put-role-policy --role-name txn-preflight-role --policy-name least-privilege --policy-document file://iam/policy-preflight.json
aws iam simulate-principal-policy --policy-source-arn arn:aws:iam::000000000000:role/txn-preflight-role \
  --action-names s3:ListBucket --resource-arns "arn:aws:s3:::txn-raw"
```

<details><summary>Verificar</summary>

Paso 1 y 4: `EvalDecision: allowed`. Paso 3: `EvalDecision: implicitDeny` — sin ningún `Allow` explícito para `s3:ListBucket`, IAM real (y `simulate-principal-policy` en MiniStack, que sí lo evalúa) niega por defecto. Nota que un statement con `Statement: []` vacío falla con `MalformedPolicyDocument` — tanto MiniStack como AWS real exigen al menos un statement, por eso el paso 2 usa un permiso *distinto* (`s3:GetBucketLocation`) en vez de dejar la política vacía. Esta es exactamente la mecánica de "validar antes de desplegar" que usarías en el trabajo real, sin depender de que el emulador bloquee nada.
</details>

**6. Demuestra por qué el outbox existe: simula un evento "perdido" y recupéralo**

Corre `python3 src/orchestration/statemachine.py` dos veces seguidas (dos `job_id` distintos, dos eventos `PENDING` nuevos en `txn-outbox`) **sin** correr `make outbox` entre medio. Luego corre `make outbox` una sola vez.

<details><summary>Verificar</summary>

`aws dynamodb scan --table-name txn-outbox` antes de `make outbox` muestra 2 (o más) filas `PENDING` acumuladas — cada `statemachine.py` dejó su evento pendiente, y nada se perdió por no haberse publicado de inmediato. Un solo `make outbox` los publica todos y los marca `PUBLISHED` — confirmá con `aws sqs receive-message` sobre `txn-curation-events-queue` que llegaron los 2 mensajes. Compará esto contra el diseño ingenuo (publicar directo dentro de `record_status.py`): ahí, si el proceso Lambda muriera entre el `put_item` y el `sns.publish`, el job quedaría marcado `completed` en DynamoDB pero el evento jamás se publicaría — y no habría manera de saberlo sin comparar manualmente contra otra fuente. Con el outbox, la fila `PENDING` es justamente esa señal: nada queda "completado" sin que su evento esté, al menos, pendiente de publicar de forma visible.
</details>

---

## 6. Quality report

```bash
make e2e
cat docs/quality-report.md
```

Las 5 dimensiones (`completeness`, `correctness`, `consistency`, `validity`, `timeliness`) miden **valor vs umbral**, no un booleano. Si el score baja, el detalle está en `benchmarks/quality-report.json`.

---

## 7. Cerrar

```bash
# mata el gate si lo dejaste en Terminal A
lsof -ti:8080 | xargs -r kill
docker compose down
```
