from __future__ import annotations

import sys
import warnings
from typing import TYPE_CHECKING, Any

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from daft.ai.provider import Provider, ProviderImportError
from daft.udf import cls as daft_cls
from daft.udf import method

if TYPE_CHECKING:
    from daft import Series
    from daft.ai.protocols import (
        ImageClassifierDescriptor,
        ImageEmbedderDescriptor,
        TextClassifierDescriptor,
    )
    from daft.ai.typing import (
        ClassifyImageOptions,
        ClassifyTextOptions,
        EmbedImageOptions,
        EmbedTextOptions,
        Options,
    )
    from daft.expressions import Expression


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

        from daft.dependencies import np, torch

        if not torch.module_available() or not np.module_available():  # type: ignore[attr-defined]
            raise ProviderImportError("transformers")

    @property
    def name(self) -> str:
        return self._name

    def get_image_embedder(
        self,
        model: str | None = None,
        **options: Unpack[EmbedImageOptions],
    ) -> ImageEmbedderDescriptor:
        from daft.dependencies import pil_image, torchvision

        if not torchvision.module_available() or not pil_image.module_available():
            raise ProviderImportError("transformers", function="embed_image")

        from daft.ai.transformers.protocols.image_embedder import (
            TransformersImageEmbedderDescriptor,
        )

        embed_options: EmbedImageOptions = options
        return TransformersImageEmbedderDescriptor(model or self.DEFAULT_IMAGE_EMBEDDER, embed_options=embed_options)

    def get_text_classifier(
        self,
        model: str | None = None,
        **options: Unpack[ClassifyTextOptions],
    ) -> TextClassifierDescriptor:
        from daft.ai.transformers.protocols.text_classifier import (
            TransformersTextClassifierDescriptor,
        )

        classify_options: ClassifyTextOptions = options
        return TransformersTextClassifierDescriptor(
            provider_name=self._name,
            model_name=(model or self.DEFAULT_TEXT_CLASSIFIER),
            classify_options=classify_options,
        )

    def create_text_embedder(
        self,
        model: str | None = None,
        dimensions: int | None = None,
        **options: Unpack[EmbedTextOptions],
    ) -> Expression:
        """Create a TextEmbedder UDF expression for the Transformers provider."""
        from daft.ai._expressions import _TextEmbedderExpression
        from daft.ai.transformers.protocols.text_embedder import (
            TransformersTextEmbedderDescriptor,
        )

        if dimensions is not None:
            warnings.warn(
                f"embed_text dimensions was specified but provider {self.name} currently ignores this property: see https://github.com/Eventual-Inc/Daft/issues/5555"
            )

        embed_options: EmbedTextOptions = options
        descriptor = TransformersTextEmbedderDescriptor(
            model or self.DEFAULT_TEXT_EMBEDDER, embed_options=embed_options
        )
        udf_options = descriptor.get_udf_options()
        return_dtype = descriptor.get_dimensions().as_dtype()

        @daft_cls(
            max_concurrency=udf_options.concurrency,
            gpus=udf_options.num_gpus or 0,
            max_retries=udf_options.max_retries,
            on_error=udf_options.on_error,
            name_override="embed_text",
        )
        class TransformersTextEmbedderExpression(_TextEmbedderExpression):
            @method.batch(
                return_dtype=return_dtype,
                batch_size=udf_options.batch_size,
            )
            def __call__(self, text_series: Series):
                return self._call_sync(text_series)

        return TransformersTextEmbedderExpression(descriptor)

    def get_image_classifier(
        self,
        model: str | None = None,
        **options: Unpack[ClassifyImageOptions],
    ) -> ImageClassifierDescriptor:
        from daft.dependencies import pil_image, torchvision

        if not torchvision.module_available() or not pil_image.module_available():
            raise ProviderImportError("transformers", function="classify_image")

        from daft.ai.transformers.protocols.image_classifier import (
            TransformersImageClassifierDescriptor,
        )

        classify_options: ClassifyImageOptions = options
        return TransformersImageClassifierDescriptor(
            provider_name=self._name,
            model_name=(model or self.DEFAULT_IMAGE_CLASSIFIER),
            classify_options=classify_options,
        )
