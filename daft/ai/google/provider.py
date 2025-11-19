from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from daft.ai.provider import Provider

if TYPE_CHECKING:
    from daft.ai.google.typing import GoogleProviderOptions
    from daft.ai.protocols import PrompterDescriptor, TextEmbedderDescriptor




class GoogleProvider(Provider):
    _name: str
    _options: GoogleProviderOptions
    DEFAULT_TEXT_EMBEDDER = "models/text-embedding-004"
    DEFAULT_PROMPTER_MODEL = "gemini-2.5-flash"

    def __init__(self, name: str | None = None, **options: Unpack[GoogleProviderOptions]):
        self._name = name if name else "google"
        self._options = options

    @property
    def name(self) -> str:
        return self._name

    def get_text_embedder(
        self, model: str | None = None, dimensions: int | None = None, **options: Any
    ) -> TextEmbedderDescriptor:
        # TODO: Implement GoogleTextEmbedderDescriptor
        raise NotImplementedError("Google text embedder not implemented yet")

    def get_prompter(self, model: str | None = None, **options: Any) -> PrompterDescriptor:
        from daft.ai.google.protocols.prompter import GooglePrompterDescriptor

        # Extract return_format from options if provided
        return_format = options.pop("return_format", None)
        system_message = options.pop("system_message", None)

        # Extract udf options from options if provided
        udf_options = options.pop("udf_options", None)

        return GooglePrompterDescriptor(
            provider_name=self._name,
            provider_options=self._options,
            model_name=(model or self.DEFAULT_PROMPTER_MODEL),
            model_options=options,
            system_message=system_message,
            return_format=return_format,
            udf_options=udf_options,
        )
