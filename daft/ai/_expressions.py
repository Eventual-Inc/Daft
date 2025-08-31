from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft import Series
    from daft.ai.protocols import ImageEmbedder, ImageEmbedderDescriptor, TextEmbedder, TextEmbedderDescriptor
    from daft.ai.typing import Embedding


class _TextEmbedderExpression:
    """Function expression implementation for a TextEmbedder protocol."""

    text_embedder: TextEmbedder

    def __init__(self, text_embedder: TextEmbedderDescriptor):
        self.text_embedder = text_embedder.instantiate()

    def __call__(self, text_series: Series) -> list[Embedding]:
        text = text_series.to_pylist()
        return self.text_embedder.embed_text(text) if text else []


class _ImageEmbedderExpression:
    """Function expression implementation for a ImageEmbedder protocol."""

    image_embedder: ImageEmbedder

    def __init__(self, image_embedder: ImageEmbedderDescriptor):
        self.image_embedder = image_embedder.instantiate()

    def __call__(self, image_series: Series) -> list[Embedding]:
        image = image_series.to_pylist()
        return self.image_embedder.embed_image(image) if image else []
