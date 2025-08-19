from __future__ import annotations

from daft.ai.provider import Provider

from daft.ai.openai.text_embedder import OpenAITextEmbedderDescriptor
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor
    from daft.ai.typing import Options

__all__ = [
    "OpenAIProvider",
]


class OpenAIProvider(Provider):
    _options: Options

    def __init__(self, **options: str):
        self._options = options

    def get_text_embedder(self, model: str | None = None, **options: str) -> TextEmbedderDescriptor:
        return OpenAITextEmbedderDescriptor(model or "text-embedding-ada-002", options)
