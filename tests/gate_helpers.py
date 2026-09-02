"""Plain (non-fixture) helpers for talking to the Go idempotency gate.

Kept separate from the `gate_process` fixture, which lives in the repo-root
`conftest.py` so pytest injects it into both `tests/` and `features/steps/`
without either having to import it — importing a fixture and then re-declaring
it as a test parameter is what produced the F811 redefinition warnings this
split removes.
"""

from pathlib import Path

GATE_URL = "http://localhost:8080"
GATE_BIN = Path(__file__).resolve().parents[1] / "src" / "ingestion" / "gate" / "gate"


def _event(idempotency_key: str, txn_id: str) -> dict:
    return {
        "txn_id": txn_id,
        "idempotency_key": idempotency_key,
        "account_id": "acc_test",
        "amount_cents": 100,
        "currency": "USD",
        "schema_version": 1,
        "ts": "2026-01-01T00:00:00Z",
    }
