from __future__ import annotations

from daft.ai.provider import Provider

from daft.ai.sentence_transformers.text_embedder import SentenceTransformersTextEmbedderDescriptor
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor

__all__ = [
    "SentenceTransformersProvider",
]


class SentenceTransformersProvider(Provider):
    def __init__(self, **options: str):
        # TODO: consider options like "backend", for now defaults to torch
        pass

    def get_text_embedder(self, model: str | None = None, **options: str) -> TextEmbedderDescriptor:
        # TODO: ASAP plumbing embedding dimensions, can also create a large default table for common models
        return SentenceTransformersTextEmbedderDescriptor(model or "all-MiniLM-L6-v2", options)
