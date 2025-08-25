from __future__ import annotations

from daft.ai.provider import Provider

from daft.ai.sentence_transformers.text_embedder import SentenceTransformersTextEmbedderDescriptor
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor
    from daft.ai.typing import Options

__all__ = [
    "SentenceTransformersProvider",
]


class SentenceTransformersProvider(Provider):
    _name: str
    _options: Options

    def __init__(self, name: str | None = None, **options: Any):
        self._name = name if name else "sentence_transformers"
        self._options = options

    @property
    def name(self) -> str:
        return self._name

    def get_text_embedder(self, model: str | None = None, **options: Any) -> TextEmbedderDescriptor:
        # TODO: ASAP plumbing embedding dimensions, can also create a large default table for common models
        return SentenceTransformersTextEmbedderDescriptor(model or "all-MiniLM-L6-v2", options)
