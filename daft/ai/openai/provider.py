from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from daft.ai.provider import Provider

if TYPE_CHECKING:
    from daft.ai.openai.typing import OpenAIProviderOptions
    from daft.ai.protocols import PrompterDescriptor, TextEmbedderDescriptor


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
            raise ImportError("OpenAI is not installed. Please install it using `pip install daft[openai]`")

        from daft.dependencies import np, pil_image

        if not np.module_available():
            raise ImportError("Numpy is not installed. Please install it using `pip install daft[openai]`")

        if not pil_image.module_available():
            raise ImportError("Pillow is not installed. Please install it using `pip install daft[openai]`")

    

    @property
    def name(self) -> str:
        return self._name

    def get_text_embedder(
        self, model: str | None = None, dimensions: int | None = None, **options: Any
    ) -> TextEmbedderDescriptor:
        from daft.ai.openai.protocols.text_embedder import OpenAITextEmbedderDescriptor

        return OpenAITextEmbedderDescriptor(
            provider_name=self._name,
            provider_options=self._options,
            model_name=(model or self.DEFAULT_TEXT_EMBEDDER),
            dimensions=dimensions,
            model_options=options,
        )

    def get_prompter(self, model: str | None = None, **options: Any) -> PrompterDescriptor:
        from daft.ai.openai.protocols.prompter import OpenAIPrompterDescriptor

        # Extract return_format from options if provided
        return_format = options.pop("return_format", None)
        system_message = options.pop("system_message", None)
        use_chat_completions = options.pop("use_chat_completions", False)

        # Extract udf options from options if provided
        udf_options = options.pop("udf_options", None)

        return OpenAIPrompterDescriptor(
            provider_name=self._name,
            provider_options=self._options,
            model_name=(model or self.DEFAULT_PROMPTER_MODEL),
            model_options=options,
            system_message=system_message,
            return_format=return_format,
            udf_options=udf_options,
            use_chat_completions=use_chat_completions,
        )
