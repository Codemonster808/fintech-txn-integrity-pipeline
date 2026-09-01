"""
Deterministic LLM stand-in: no network, no cost, reproducible output.
Used by `make demo`, pytest, and CI. Real metrics come from the
minimax provider via `make eval`, never from this one.
"""

import hashlib
import json


class FakeLLMProvider:
    def complete(self, prompt: str, *, max_tokens: int = 512) -> str:
        digest = hashlib.sha256(prompt.encode()).hexdigest()[:8]
        return json.dumps(
            {
                "answer": f"[fake-llm deterministic response {digest}]",
                "confidence": 0.5,
                "citations": [],
            }
        )
