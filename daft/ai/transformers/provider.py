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
        TextClassifierDescriptor,
        TextEmbedderDescriptor,
    )
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
    DEFAULT_PROMPTER = "Qwen/Qwen3-VL-2B-instruct"

    def __init__(self, name: str | None = None, **options: Any):
        self._name = name if name else "transformers"
        self._options = options

        from daft.dependencies import np, torch, transformers

        if (
            not torch.module_available()
            or not np.module_available()  # type: ignore[attr-defined]
            or not transformers.module_available()
        ):
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

        if dimensions is not None:
            warnings.warn(
                f"embed_text dimensions was specified but provider {self.name} currently ignores this property: see https://github.com/Eventual-Inc/Daft/issues/5555"
            )

        embed_options: EmbedTextOptions = options
        return TransformersTextEmbedderDescriptor(model or self.DEFAULT_TEXT_EMBEDDER, embed_options=embed_options)

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
        return_format: type[BaseModel] | None = None,
        system_message: str | None = None,
        **options: Any,
    ) -> Any:  # Returns expression, not descriptor - will be refactored
        from daft.ai.transformers.protocols.prompter import TransformersPrompterDescriptor
        ## ==============================================
        ## Proposal
        ## ==============================================

        ## Parse UDF Options from options
        # udf_options = from_dict(UDFOptions, options)

        ## Determine return dtype based on return_format
        # if return_format is not None:
        #    if IS_COLAB:
        #        # Clean the Pydantic model to avoid Colab serialization issues
        #        return_format = clean_pydantic_model(return_format)
        #    try:
        #        return_dtype = DataType.infer_from_type(return_format)
        #    except Exception:
        #        return_dtype = DataType.string()
        # else:
        #    return_dtype = DataType.string()

        ## Clearer Expression building for UDFs
        # @daft_cls(
        #    gpus=udf_options.num_gpus or 0,
        #    max_concurrency=udf_options.concurrency,
        #    max_retries=udf_options.max_retries,
        #    on_error=udf_options.on_error,
        #    use_process=True,
        # )
        # class TransformersPrompterExpression(_PrompterExpression):
        #    """Function expression implementation for a Prompter protocol."""

        #    def __init__(self, descriptor: TransformersPrompterDescriptor):
        #        self.prompter = TransformersPrompter(descriptor)

        #    @daft_method(return_dtype=return_dtype)
        #    async def prompt(self, *messages: Any) -> Any:
        #        return await self.prompter.prompt(messages)

        # descriptor = TransformersPrompterDescriptor(
        #    provider_name=self._name,
        #    model_name=(model or self.DEFAULT_PROMPTER),
        #    system_message=system_message,
        #    return_format=return_format,
        #    prompt_options=(options),
        # )

        ## Instantiate the Expression with the descriptor
        # expression = TransformersPrompterExpression(descriptor)

        # return expression
        return TransformersPrompterDescriptor(
            provider_name=self._name,
            model_name=(model or self.DEFAULT_PROMPTER),
            system_message=system_message,
            return_format=return_format,
            prompt_options=(options),
        )
