from typing import Protocol


class LLMProvider(Protocol):
    def complete(self, prompt: str, *, max_tokens: int = 512) -> str:
        """Return a text completion for the given prompt."""
        ...
