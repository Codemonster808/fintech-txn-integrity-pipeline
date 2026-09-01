"""
MiniMax M3 via its OpenAI-compatible endpoint. Not an Anthropic model,
so this uses the `openai` client, not the Anthropic SDK.

Costs real money per call (~$0.30/M input tokens, ~$1.20/M output as of
this writing) — only reached when LLM_PROVIDER=minimax, e.g. `make eval`.
"""

import os
import re

from openai import OpenAI

_MODEL = "MiniMax-M3"
_BASE_URL = "https://api.minimax.io/v1"
_THINK_TAG_RE = re.compile(r"<think>.*?</think>", re.DOTALL)

# MiniMax-M3 is a reasoning model: it always spends tokens on a <think>...</think>
# block before the actual answer. A max_tokens that's too small gets consumed
# entirely by the thinking trace, leaving an empty answer — found by actually
# calling the API, not documented anywhere obvious beforehand.
_MIN_TOKENS_FOR_THINKING_HEADROOM = 300


class MiniMaxLLMProvider:
    def __init__(self) -> None:
        api_key = os.environ.get("MINIMAX_API_KEY")
        if not api_key:
            raise RuntimeError(
                "MINIMAX_API_KEY not set. Copy it from ~/.config/de-portfolio/.env "
                "or export it before running with LLM_PROVIDER=minimax."
            )
        self._client = OpenAI(api_key=api_key, base_url=_BASE_URL)

    def complete(self, prompt: str, *, max_tokens: int = 512) -> str:
        response = self._client.chat.completions.create(
            model=_MODEL,
            max_tokens=max(max_tokens, _MIN_TOKENS_FOR_THINKING_HEADROOM),
            messages=[{"role": "user", "content": prompt}],
        )
        content = response.choices[0].message.content or ""
        return _THINK_TAG_RE.sub("", content).strip()
