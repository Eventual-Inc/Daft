from __future__ import annotations

from daft.ai.provider import Provider

from daft.ai.openai.text_embedder import OpenAITextEmbedderDescriptor, LMStudioTextEmbedderDescriptor
from typing import TYPE_CHECKING, Any, TypedDict

from typing_extensions import Unpack

if TYPE_CHECKING:
    from daft.ai.openai.typing import OpenAIProviderOptions
    from daft.ai.protocols import ImageEmbedderDescriptor, TextEmbedderDescriptor
    from daft.ai.typing import Options

__all__ = [
    "LMStudioProvider",
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

    def get_image_embedder(self, model: str | None = None, **options: Any) -> ImageEmbedderDescriptor:
        raise NotImplementedError("embed_image is not currently implemented for the OpenAI provider")


class LMStudioProvider(OpenAIProvider):
    """LM Studio provider that extends OpenAI provider with local server configuration.

    LM Studio runs a local server that's API-compatible with OpenAI, so we can reuse
    all the OpenAI logic and just configure the base URL to point to the local instance.
    """

    def __init__(
        self,
        name: str | None = None,
        **options: Unpack[OpenAIProviderOptions],
    ):
        if "api_key" not in options:
            options["api_key"] = "not-needed-for-lm-studio"
        if "base_url" not in options:
            options["base_url"] = "http://localhost:1234/v1"
        else:
            # Ensure base_url ends with /v1 for LM Studio compatibility.
            base_url = options["base_url"]
            if base_url is not None and not base_url.endswith("/v1"):
                options["base_url"] = base_url.rstrip("/") + "/v1"
        super().__init__(name or "lm_studio", **options)

    def get_text_embedder(self, model: str | None = None, **options: Any) -> TextEmbedderDescriptor:
        return LMStudioTextEmbedderDescriptor(
            provider_name=self._name,
            provider_options=self._options,
            model_name=(model or "text-embedding-3-small"),
            model_options=options,
        )

    def get_image_embedder(self, model: str | None = None, **options: Any) -> ImageEmbedderDescriptor:
        raise NotImplementedError("embed_image is not currently implemented for the LM Studio provider")
