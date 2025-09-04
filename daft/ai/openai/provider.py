from __future__ import annotations

from typing import TYPE_CHECKING, Any

from typing_extensions import Unpack

from daft.ai.provider import Provider

if TYPE_CHECKING:
    from daft.ai.openai.typing import OpenAIProviderOptions
    from daft.ai.protocols import TextEmbedderDescriptor


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
        from daft.ai.openai.protocols.text_embedder import OpenAITextEmbedderDescriptor

        return OpenAITextEmbedderDescriptor(
            provider_name=self._name,
            provider_options=self._options,
            model_name=(model or "text-embedding-3-small"),
            model_options=options,
        )
