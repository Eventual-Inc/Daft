from __future__ import annotations

from daft.ai.provider import Provider

from daft.ai.openai.text_embedder import OpenAITextEmbedderDescriptor
from typing import TYPE_CHECKING, Any, TypedDict

from typing_extensions import Unpack

if TYPE_CHECKING:
    from daft.ai.openai.typing import OpenAIProviderOptions
    from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor
    from daft.ai.typing import Options

__all__ = [
    "OpenAIProvider",
]


class OpenAIProvider(Provider):
    _name: str
    _options: OpenAIProviderOptions

    def __init__(self, name: str | None = None, **options: Unpack[OpenAIProviderOptions]):
        self._name = name if name else "openai"
        self._options = options

    @property
    def name(self) -> str:
        return self._name

    def get_text_embedder(self, model: str | None = None, **options: Any) -> TextEmbedderDescriptor:
        return OpenAITextEmbedderDescriptor(
            provider_name=self._name,
            provider_options=self._options,
            model_name=(model or "text-embedding-3-small"),
            model_options=options,
        )
