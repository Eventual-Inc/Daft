from __future__ import annotations

import sys
import warnings
from typing import TYPE_CHECKING, Any

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from daft.ai.provider import Provider, ProviderImportError

if TYPE_CHECKING:
    from pydantic import BaseModel

    from daft.ai.protocols import (
        ImageClassifierDescriptor,
        ImageEmbedderDescriptor,
        PrompterDescriptor,
        TextClassifierDescriptor,
        TextEmbedderDescriptor,
    )
    from daft.ai.transformers.protocols.prompter import TransformersPromptOptions
    from daft.ai.typing import (
        ClassifyImageOptions,
        ClassifyTextOptions,
        EmbedImageOptions,
        EmbedTextOptions,
        Options,
    )


class TransformersProvider(Provider):
    _name: str
    _options: Options
    DEFAULT_IMAGE_EMBEDDER = "openai/clip-vit-base-patch32"
    DEFAULT_TEXT_EMBEDDER = "sentence-transformers/all-MiniLM-L6-v2"
    DEFAULT_TEXT_CLASSIFIER = "facebook/bart-large-mnli"
    DEFAULT_IMAGE_CLASSIFIER = "openai/clip-vit-base-patch32"
    DEFAULT_PROMPTER = "HuggingFaceTB/SmolLM2-360M-Instruct"

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

    def get_text_embedder(
        self,
        model: str | None = None,
        dimensions: int | None = None,
        **options: Unpack[EmbedTextOptions],
    ) -> TextEmbedderDescriptor:
        from daft.ai.transformers.protocols.text_embedder import (
            TransformersTextEmbedderDescriptor,
        )

        embed_options: EmbedTextOptions = options
        return TransformersTextEmbedderDescriptor(
            model=model or self.DEFAULT_TEXT_EMBEDDER,
            dimensions=dimensions,
            embed_options=embed_options,
        )

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

    def get_prompter(
        self,
        model: str | None = None,
        return_format: BaseModel | None = None,
        system_message: str | None = None,
        **options: Unpack[TransformersPromptOptions],
    ) -> PrompterDescriptor:
        model_name = model or self.DEFAULT_PROMPTER
        if self._is_vision_model(model_name):
            from daft.ai.transformers.protocols.vision_prompter import (
                TransformersVisionPrompterDescriptor,
            )

            return TransformersVisionPrompterDescriptor(
                provider_name=self._name,
                model_name=model_name,
                prompt_options=options,
                system_message=system_message,
                return_format=return_format,
            )

        from daft.ai.transformers.protocols.prompter import TransformersPrompterDescriptor

        return TransformersPrompterDescriptor(
            provider_name=self._name,
            model_name=model_name,
            prompt_options=options,
            system_message=system_message,
            return_format=return_format,
        )

    @staticmethod
    def _is_vision_model(model_name: str) -> bool:
        """Warn and return False on any lookup failure so the caller falls back to text.

        Tries the local HF cache first so repeat / offline calls avoid a Hub round-trip;
        only reaches out to the Hub when the config has never been downloaded.
        """
        try:
            from transformers import AutoConfig

            # The names mapping is not publicly re-exported; use the same internal path as HF's AutoModel dispatch.
            from transformers.models.auto.modeling_auto import (
                MODEL_FOR_IMAGE_TEXT_TO_TEXT_MAPPING_NAMES,
            )

            try:
                config = AutoConfig.from_pretrained(model_name, local_files_only=True)
            except OSError:
                config = AutoConfig.from_pretrained(model_name)
            return config.model_type in MODEL_FOR_IMAGE_TEXT_TO_TEXT_MAPPING_NAMES
        except Exception as e:
            warnings.warn(
                f"Could not determine vision capability for model '{model_name}' ({e!r}); "
                "falling back to the text-only prompter. Pass an instruction-tuned VLM name "
                "if you need image support.",
                stacklevel=3,
            )
            return False
