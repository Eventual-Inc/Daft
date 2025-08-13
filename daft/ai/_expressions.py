from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft import Series
    from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor
    from daft.ai.typing import Embedding


class _TextEmbedderExpression:
    """Function expression implementation for a TextEmbedder protocol."""

    text_embedder: TextEmbedder

    def __init__(self, text_embedder: TextEmbedderDescriptor):
        self.text_embedder = text_embedder.instantiate()

    def __call__(self, text_series: Series) -> list[Embedding]:
        text = text_series.to_pylist()
        return self.text_embedder.embed_text(text) if text else []
