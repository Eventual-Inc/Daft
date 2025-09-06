from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.ai.provider import Provider

if TYPE_CHECKING:
    from daft.ai.protocols import ImageEmbedderDescriptor, TextClassifierDescriptor
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

    def get_text_classifier(self, model: str | None = None, **options: Any) -> TextClassifierDescriptor:
        from daft.ai.transformers.protocols.text_classifier import (
            TransformersTextClassifierDescriptor,
            TransformersTextClassifierOptions,
        )

        model_options = {k: v for k, v in options.items() if k in TransformersTextClassifierOptions.__annotations__}

        return TransformersTextClassifierDescriptor(
            provider_name=self._name,
            model_name=(model or "facebook/bart-large-mnli"),
            model_options=model_options,  # type: ignore
        )
