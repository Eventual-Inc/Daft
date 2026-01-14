from __future__ import annotations

import sys
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
    from daft.ai.openai.protocols.prompter import OpenAIPromptOptions
    from daft.ai.openai.typing import OpenAIProviderOptions
    from daft.ai.protocols import PrompterDescriptor
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

    def create_text_embedder(self, model: str | None = None, dimensions: int | None = None, **options: Any) -> Expression:
        """Create a TextEmbedder UDF expression for the OpenAI provider."""
        from daft.ai._expressions import _TextEmbedderExpression
        from daft.ai.openai.protocols.text_embedder import OpenAITextEmbedderDescriptor

        descriptor = OpenAITextEmbedderDescriptor(
            provider_name=self._name,
            provider_options=self._options,
            model_name=(model or self.DEFAULT_TEXT_EMBEDDER),
            dimensions=dimensions,
            embed_options=options,
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
        class OpenAITextEmbedderExpression(_TextEmbedderExpression):
            @method.batch(
                return_dtype=return_dtype,
                batch_size=udf_options.batch_size,
            )
            async def __call__(self, text_series: Series):
                return await self._call_async(text_series)

        return OpenAITextEmbedderExpression(descriptor)

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
