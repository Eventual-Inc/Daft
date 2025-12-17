from __future__ import annotations

import sys
import warnings
from typing import TYPE_CHECKING

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from daft.ai.openai.provider import OpenAIProvider

if TYPE_CHECKING:
    from daft.ai.openai.typing import OpenAIProviderOptions
    from daft.ai.protocols import TextEmbedderDescriptor
    from daft.ai.typing import EmbedTextOptions


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

    def get_text_embedder(
        self,
        model: str | None = None,
        dimensions: int | None = None,
        **options: Unpack[EmbedTextOptions],
    ) -> TextEmbedderDescriptor:
        from daft.ai.lm_studio.protocols.text_embedder import (
            LMStudioTextEmbedderDescriptor,
        )

        if dimensions is not None:
            warnings.warn(
                f"embed_text dimensions was specified but provider {self.name} currently ignores this property: see https://github.com/Eventual-Inc/Daft/issues/5555"
            )

        return LMStudioTextEmbedderDescriptor(
            provider_name=self._name,
            provider_options=self._options,
            model_name=(model or self.DEFAULT_TEXT_EMBEDDER),
            embed_options=options,
        )
