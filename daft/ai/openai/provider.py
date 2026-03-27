from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from daft.ai.provider import Provider, ProviderImportError

if TYPE_CHECKING:
    from daft.ai.openai.protocols.prompter import OpenAIPromptOptions
    from daft.ai.openai.protocols.text_embedder import OpenAITextEmbedderDescriptor
    from daft.ai.openai.typing import OpenAIProviderOptions
    from daft.ai.protocols import PrompterDescriptor, TextEmbedder, TextEmbedderDescriptor
    from daft.ai.typing import EmbedTextOptions, UDFOptions
    from daft.expressions import Expression


class OpenAIProvider(Provider):
    _name: str
    _options: OpenAIProviderOptions
    DEFAULT_TEXT_EMBEDDER = "text-embedding-3-small"
    DEFAULT_PROMPTER_MODEL = "gpt-4o-mini"

    def __init__(self, name: str | None = None, **options: Unpack[OpenAIProviderOptions]):
        self._name = name if name else "openai"
        self._options = options

        try:
            import openai  # noqa: F401
        except ImportError:
            raise ProviderImportError("openai")

        from daft.dependencies import np

        if not np.module_available():  # type: ignore[attr-defined]
            raise ProviderImportError("openai")

    @property
    def name(self) -> str:
        return self._name

    def _get_udf_options(self, descriptor: TextEmbedderDescriptor) -> UDFOptions:
        """Overrides the default UDF options to set max_retries to 0."""
        udf_options = super()._get_udf_options(descriptor)
        udf_options.max_retries = 0  # OpenAI client handles retries internally
        return udf_options

    # ------------------------------------------------------------------
    # OpenAI Text Embedder
    # ------------------------------------------------------------------

    def get_text_embedder_descriptor(
        self, model: str | None = None, dimensions: int | None = None, **options: Unpack[EmbedTextOptions]
    ) -> OpenAITextEmbedderDescriptor:
        """Validates options and returns a fully-resolved OpenAITextEmbedderDescriptor."""
        from daft.ai.openai.protocols.text_embedder import (
            OpenAITextEmbedderDescriptor,
            _resolve_dimensions,
            _validate_model,
            get_input_text_token_limit_for_model,
        )
        from daft.ai.utils import merge_provider_and_api_options

        model_name = model or self.DEFAULT_TEXT_EMBEDDER
        embed_options: EmbedTextOptions = options
        supports_overriding_dimensions = False

        # Validate model for standard OpenAI (no custom base_url)
        if self._options.get("base_url") is None:
            embed_options = _validate_model(model_name, dimensions, embed_options)
            supports_overriding_dimensions = embed_options.get("supports_overriding_dimensions", False)

        resolved_dims = _resolve_dimensions(model_name, dimensions, self._options, embed_options)

        # Merge provider + embed options into the final client options
        from daft.ai.openai.typing import OpenAIProviderOptions as OPO

        merged_provider_options: dict[str, Any] = merge_provider_and_api_options(
            provider_options=self._options,
            api_options=embed_options,
            provider_option_type=OPO,
        )

        return OpenAITextEmbedderDescriptor(
            provider=self._name,
            model=model_name,
            dimensions=resolved_dims,
            embed_options=embed_options,
            provider_options=merged_provider_options,
            supports_overriding_dimensions=supports_overriding_dimensions,
            batch_token_limit=embed_options.get("batch_token_limit", 300_000),
            input_text_token_limit=get_input_text_token_limit_for_model(model_name),
        )

    def get_text_embedder(self, descriptor: OpenAITextEmbedderDescriptor) -> TextEmbedder:  # type: ignore[override]
        from daft.ai.openai.protocols.text_embedder import OpenAITextEmbedder

        return OpenAITextEmbedder(descriptor)

    def embed_text(
        self,
        text: Expression,
        model: str | None = None,
        dimensions: int | None = None,
        **options: Unpack[EmbedTextOptions],
    ) -> Expression:
        from daft.ai._expressions import _TextEmbedderExpression
        from daft.udf import cls as daft_cls
        from daft.udf import method

        descriptor = self.get_text_embedder_descriptor(model, dimensions, **options)
        udf_options = self._get_udf_options(descriptor)

        _TextEmbedderExpression.__call__ = method.batch(  # type: ignore[method-assign]
            method=_TextEmbedderExpression._call_async,
            return_dtype=descriptor["dimensions"].as_dtype(),
            batch_size=udf_options.batch_size,
        )
        wrapped_cls = daft_cls(
            _TextEmbedderExpression,
            max_concurrency=udf_options.concurrency,
            gpus=udf_options.num_gpus or 0,
            max_retries=udf_options.max_retries,
            on_error=udf_options.on_error,
            name_override="embed_text",
        )

        return wrapped_cls(descriptor)(text)

    # ------------------------------------------------------------------
    # OpenAI Prompter
    # ------------------------------------------------------------------

    def get_prompter(
        self,
        model: str | None = None,
        return_format: Any | None = None,
        system_message: str | None = None,
        **options: Unpack[OpenAIPromptOptions],
    ) -> PrompterDescriptor:
        from daft.ai.openai.protocols.prompter import OpenAIPrompterDescriptor

        return OpenAIPrompterDescriptor(
            provider_name=self._name,
            provider_options=self._options,
            model_name=(model or self.DEFAULT_PROMPTER_MODEL),
            return_format=return_format,
            system_message=system_message,
            prompt_options=options,
        )
