from __future__ import annotations

import sys
import warnings
from typing import TYPE_CHECKING, Any, cast

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from daft.ai.provider import Provider

if TYPE_CHECKING:
    from daft.ai.protocols import (
        ImageClassifierDescriptor,
        ImageEmbedderDescriptor,
        TextClassifierDescriptor,
        TextEmbedderDescriptor,
    )
    from daft.ai.transformers.protocols.image_classifier import (
        TransformersImageClassifierOptions,
    )
    from daft.ai.transformers.protocols.image_embedder import (
        TransformersImageEmbedderOptions,
    )
    from daft.ai.transformers.protocols.text_classifier import (
        TransformersTextClassifierOptions,
    )
    from daft.ai.transformers.protocols.text_embedder import (
        TransformersTextEmbedderOptions,
    )
    from daft.ai.typing import Options


class TransformersProvider(Provider):
    _name: str
    _options: Options
    DEFAULT_IMAGE_EMBEDDER = "openai/clip-vit-base-patch32"
    DEFAULT_TEXT_EMBEDDER = "sentence-transformers/all-MiniLM-L6-v2"
    DEFAULT_TEXT_CLASSIFIER = "facebook/bart-large-mnli"
    DEFAULT_IMAGE_CLASSIFIER = "openai/clip-vit-base-patch32"

    def __init__(self, name: str | None = None, **options: Any):
        self._name = name if name else "transformers"
        self._options = options

    @property
    def name(self) -> str:
        return self._name

    def get_image_embedder(
        self,
        model: str | None = None,
        **options: Unpack[TransformersImageEmbedderOptions],
    ) -> ImageEmbedderDescriptor:
        from daft.ai.transformers.protocols.image_embedder import (
            TransformersImageEmbedderDescriptor,
        )

        return TransformersImageEmbedderDescriptor(
            model or self.DEFAULT_IMAGE_EMBEDDER, cast("TransformersImageEmbedderOptions", options)
        )

    def get_text_classifier(
        self,
        model: str | None = None,
        **options: Unpack[TransformersTextClassifierOptions],
    ) -> TextClassifierDescriptor:
        from daft.ai.transformers.protocols.text_classifier import (
            TransformersTextClassifierDescriptor,
        )

        return TransformersTextClassifierDescriptor(
            provider_name=self._name,
            model_name=(model or self.DEFAULT_TEXT_CLASSIFIER),
            model_options=cast("TransformersTextClassifierOptions", options),
        )

    def get_text_embedder(
        self,
        model: str | None = None,
        dimensions: int | None = None,
        **options: Unpack[TransformersTextEmbedderOptions],
    ) -> TextEmbedderDescriptor:
        from daft.ai.transformers.protocols.text_embedder import (
            TransformersTextEmbedderDescriptor,
        )

        if dimensions is not None:
            warnings.warn(
                f"embed_text dimensions was specified but provider {self.name} currently ignores this property: see https://github.com/Eventual-Inc/Daft/issues/5555"
            )

        return TransformersTextEmbedderDescriptor(
            model or self.DEFAULT_TEXT_EMBEDDER, cast("TransformersTextEmbedderOptions", options)
        )

    def get_image_classifier(
        self,
        model: str | None = None,
        **options: Unpack[TransformersImageClassifierOptions],
    ) -> ImageClassifierDescriptor:
        from daft.ai.transformers.protocols.image_classifier import (
            TransformersImageClassifierDescriptor,
        )

        return TransformersImageClassifierDescriptor(
            provider_name=self._name,
            model_name=(model or self.DEFAULT_IMAGE_CLASSIFIER),
            model_options=cast("TransformersImageClassifierOptions", options),
        )
