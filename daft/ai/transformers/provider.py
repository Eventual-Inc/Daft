from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from daft.ai.protocols import TextEmbedderDescriptor
from daft.ai.provider import Provider, ProviderImportError

if TYPE_CHECKING:
    from daft.ai.protocols import (
        ImageClassifierDescriptor,
        ImageEmbedderDescriptor,
        TextClassifierDescriptor,
        TextEmbedder,
    )
    from daft.ai.typing import (
        ClassifyImageOptions,
        ClassifyTextOptions,
        EmbedImageOptions,
        EmbedTextOptions,
        Options,
        UDFOptions,
    )


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

    def _get_udf_options(self, descriptor: TextEmbedderDescriptor) -> UDFOptions:
        """Overrides the default UDF options with GPU-aware settings."""
        from daft.ai.utils import get_gpu_udf_options

        udf_options = get_gpu_udf_options()
        for key, value in descriptor.get("embed_options", {}).items():
            if key in udf_options.__annotations__:
                setattr(udf_options, key, value)
        return udf_options

    # ------------------------------------------------------------------
    # Transformers Text Embedder
    # ------------------------------------------------------------------

    def get_text_embedder_descriptor(
        self,
        model: str | None = None,
        dimensions: int | None = None,
        **options: Unpack[EmbedTextOptions],
    ) -> TextEmbedderDescriptor:
        """Validates options and returns a serializable TextEmbedderDescriptor."""
        from daft.ai.transformers.protocols.text_embedder import resolve_dimensions

        model_name = model or self.DEFAULT_TEXT_EMBEDDER
        embed_options: EmbedTextOptions = options

        if dimensions is not None:
            if dimensions <= 0:
                raise ValueError("Embedding dimensions must be a positive integer.")

        resolved_dims = resolve_dimensions(model_name, dimensions)

        return TextEmbedderDescriptor(
            provider=self._name,
            model=model_name,
            dimensions=resolved_dims,
            embed_options=embed_options,
        )

    def get_text_embedder(self, descriptor: TextEmbedderDescriptor) -> TextEmbedder:
        """Returns a TransformersTextEmbedder implementation from the descriptor."""
        from daft.ai.transformers.protocols.text_embedder import TransformersTextEmbedder

        return TransformersTextEmbedder(
            descriptor["model"],
            dimensions=descriptor["dimensions"].size,
            **descriptor.get("embed_options", {}),
        )

    # ------------------------------------------------------------------
    # Transformers Image Embedder
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Transformers Text Classifier
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Transformers Image Classifier
    # ------------------------------------------------------------------

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
