from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.ai.provider import Provider

if TYPE_CHECKING:
    from daft.ai.protocols import ImageEmbedderDescriptor, TextClassifierDescriptor, TextEmbedderDescriptor
    from daft.ai.typing import Options


class TransformersProvider(Provider):
    _name: str
    _options: Options
    DEFAULT_IMAGE_EMBEDDER = "openai/clip-vit-base-patch32"
    DEFAULT_TEXT_EMBEDDER = "sentence-transformers/all-MiniLM-L6-v2"
    DEFAULT_TEXT_CLASSIFIER = "facebook/bart-large-mnli"

    def __init__(self, name: str | None = None, **options: Any):
        self._name = name if name else "transformers"
        self._options = options

    @property
    def name(self) -> str:
        return self._name

    def get_image_embedder(self, model: str | None = None, **options: Any) -> ImageEmbedderDescriptor:
        from daft.ai.transformers.protocols.image_embedder import TransformersImageEmbedderDescriptor

        return TransformersImageEmbedderDescriptor(model or self.DEFAULT_IMAGE_EMBEDDER, options)

    def get_text_classifier(self, model: str | None = None, **options: Any) -> TextClassifierDescriptor:
        from daft.ai.transformers.protocols.text_classifier import (
            TransformersTextClassifierDescriptor,
            TransformersTextClassifierOptions,
        )

        model_options = {k: v for k, v in options.items() if k in TransformersTextClassifierOptions.__annotations__}

        return TransformersTextClassifierDescriptor(
            provider_name=self._name,
            model_name=(model or self.DEFAULT_TEXT_CLASSIFIER),
            model_options=model_options,  # type: ignore
        )

    def get_text_embedder(self, model: str | None = None, **options: Any) -> TextEmbedderDescriptor:
        from daft.ai.transformers.protocols.text_embedder import TransformersTextEmbedderDescriptor

        return TransformersTextEmbedderDescriptor(model or self.DEFAULT_TEXT_EMBEDDER, options)
