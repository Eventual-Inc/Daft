from __future__ import annotations

from daft.ai.provider import Provider

from daft.ai.transformers.image_embedder import TransformersImageEmbedderDescriptor
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from daft.ai.protocols import ImageEmbedderDescriptor, TextEmbedderDescriptor
    from daft.ai.typing import Options

__all__ = [
    "TransformersProvider",
]


class TransformersProvider(Provider):
    _name: str
    _options: Options

    def __init__(self, name: str | None = None, **options: Any):
        self._name = name if name else "transformers"
        self._options = options

    @property
    def name(self) -> str:
        return self._name

    def get_image_embedder(self, model: str | None = None, **options: Any) -> ImageEmbedderDescriptor:
        return TransformersImageEmbedderDescriptor(model or "openai/clip-vit-base-patch32", options)

    def get_text_embedder(self, model: str | None = None, **options: Any) -> TextEmbedderDescriptor:
        raise NotImplementedError("embed_text is not currently implemented for the Transformers provider")
