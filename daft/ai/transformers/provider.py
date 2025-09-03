from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.ai.provider import Provider

if TYPE_CHECKING:
    from daft.ai.protocols import ImageEmbedderDescriptor
    from daft.ai.typing import Options


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
        from daft.ai.transformers.protocols.image_embedder import TransformersImageEmbedderDescriptor

        return TransformersImageEmbedderDescriptor(model or "openai/clip-vit-base-patch32", options)
