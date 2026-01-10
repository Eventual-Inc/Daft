from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from daft.ai.provider import Provider, ProviderImportError

if TYPE_CHECKING:
    from daft.ai.google.typing import GoogleProviderOptions
    from daft.ai.protocols import PrompterDescriptor, TextEmbedderDescriptor
    from daft.ai.typing import EmbedTextOptions, PromptOptions


class GoogleProvider(Provider):
    _name: str
    _options: GoogleProviderOptions
    DEFAULT_TEXT_EMBEDDER = "gemini-embedding-001"
    DEFAULT_PROMPTER_MODEL = "gemini-2.5-flash"

    def __init__(self, name: str | None = None, **options: Unpack[GoogleProviderOptions]):
        self._name = name if name else "google"
        self._options = options

        try:
            from google import genai  # noqa: F401
        except ImportError:
            raise ProviderImportError("google")

        from daft.dependencies import np

        if not np.module_available():  # type: ignore[attr-defined]
            raise ProviderImportError("google")

    @property
    def name(self) -> str:
        return self._name

    def get_text_embedder(
        self,
        model: str | None = None,
        dimensions: int | None = None,
        **options: Unpack[EmbedTextOptions],
    ) -> TextEmbedderDescriptor:
        from daft.ai.google.protocols.text_embedder import GoogleTextEmbedderDescriptor

        return GoogleTextEmbedderDescriptor(
            provider_name=self._name,
            provider_options=self._options,
            model_name=(model or self.DEFAULT_TEXT_EMBEDDER),
            dimensions=dimensions,
            embed_options=options,
        )

    def get_prompter(
        self,
        model: str | None = None,
        return_format: Any | None = None,
        system_message: str | None = None,
        **options: Unpack[PromptOptions],
    ) -> PrompterDescriptor:
        from daft.ai.google.protocols.prompter import GooglePrompterDescriptor

        return GooglePrompterDescriptor(
            provider_name=self._name,
            provider_options=self._options,
            model_name=(model or self.DEFAULT_PROMPTER_MODEL),
            return_format=return_format,
            system_message=system_message,
            prompt_options=options,
        )
