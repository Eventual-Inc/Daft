from __future__ import annotations

import sys
import warnings
from typing import TYPE_CHECKING

if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from daft.ai.openai.provider import OpenAIProvider
from daft.udf import cls as daft_cls
from daft.udf import method

if TYPE_CHECKING:
    from daft import Series
    from daft.ai.openai.typing import OpenAIProviderOptions
    from daft.ai.typing import EmbedTextOptions
    from daft.expressions import Expression


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

    def create_text_embedder(
        self,
        model: str | None = None,
        dimensions: int | None = None,
        **options: Unpack[EmbedTextOptions],
    ) -> Expression:
        """Create a TextEmbedder UDF expression for the LM Studio provider."""
        from daft.ai._expressions import _TextEmbedderExpression
        from daft.ai.lm_studio.protocols.text_embedder import (
            LMStudioTextEmbedderDescriptor,
        )

        if dimensions is not None:
            warnings.warn(
                f"embed_text dimensions was specified but provider {self.name} currently ignores this property: see https://github.com/Eventual-Inc/Daft/issues/5555"
            )

        descriptor = LMStudioTextEmbedderDescriptor(
            provider_name=self._name,
            provider_options=self._options,
            model_name=(model or self.DEFAULT_TEXT_EMBEDDER),
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
        class LMStudioTextEmbedderExpression(_TextEmbedderExpression):
            @method.batch(
                return_dtype=return_dtype,
                batch_size=udf_options.batch_size,
            )
            async def __call__(self, text_series: Series):
                return await self._call_async(text_series)

        return LMStudioTextEmbedderExpression(descriptor)
