from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from openai import NOT_GIVEN, AsyncOpenAI, OpenAIError, RateLimitError
from openai.types.create_embedding_response import Usage

from daft import DataType
from daft.ai.metrics import record_token_metrics
from daft.ai.openai.typing import OpenAIProviderOptions
from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor
from daft.ai.typing import EmbeddingDimensions, EmbedTextOptions, Options, UDFOptions
from daft.ai.utils import merge_provider_and_api_options
from daft.dependencies import np

if TYPE_CHECKING:
    from openai.types import EmbeddingModel
    from openai.types.create_embedding_response import CreateEmbeddingResponse

    from daft.ai.typing import Embedding


@dataclass(frozen=True)
class _ModelProfile:
    """Model profiles contain various model-specific metadata.

    Note:
        This is a bit simpler than OO-inheritance to model the subtle
        differences between the models. If there is a need for different
        implementations, then it would make sense to
    """

    dimensions: EmbeddingDimensions
    supports_overriding_dimensions: bool


_models: dict[EmbeddingModel, _ModelProfile] = {
    "text-embedding-ada-002": _ModelProfile(
        dimensions=EmbeddingDimensions(
            size=1536,
            dtype=DataType.float32(),
        ),
        supports_overriding_dimensions=False,
    ),
    "text-embedding-3-small": _ModelProfile(
        dimensions=EmbeddingDimensions(
            size=1536,
            dtype=DataType.float32(),
        ),
        supports_overriding_dimensions=True,
    ),
    "text-embedding-3-large": _ModelProfile(
        dimensions=EmbeddingDimensions(
            size=3072,
            dtype=DataType.float32(),
        ),
        supports_overriding_dimensions=True,
    ),
}


@dataclass
class OpenAITextEmbedderDescriptor(TextEmbedderDescriptor):
    provider_name: str
    provider_options: OpenAIProviderOptions
    model_name: str
    dimensions: int | None
    embed_options: EmbedTextOptions = field(
        default_factory=lambda: EmbedTextOptions(batch_size=64, max_retries=3, on_error="raise")
    )

    def __post_init__(self) -> None:
        if self.provider_options.get("base_url") is None:
            if self.model_name not in _models:
                supported_models = ", ".join(_models.keys())
                raise ValueError(
                    f"Unsupported OpenAI embedding model '{self.model_name}', expected one of: {supported_models}"
                )
            model = _models[self.model_name]
            if self.dimensions is not None and not model.supports_overriding_dimensions:
                raise ValueError(f"OpenAI embedding model '{self.model_name}' does not support specifying dimensions")

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return dict(self.embed_options)

    def get_dimensions(self) -> EmbeddingDimensions:
        if self.dimensions is not None:
            return EmbeddingDimensions(size=self.dimensions, dtype=DataType.float32())
        return _models[self.model_name].dimensions

    def get_udf_options(self) -> UDFOptions:
        options = super().get_udf_options()
        options.max_retries = 0  # OpenAI client handles retries internally
        return options

    def is_async(self) -> bool:
        return True

    def instantiate(self) -> TextEmbedder:
        return OpenAITextEmbedder(
            provider_options=self.provider_options,
            model=self.model_name,
            embed_options=self.embed_options,
            dimensions=self.dimensions,
            provider_name=self.provider_name,
        )


class OpenAITextEmbedder(TextEmbedder):
    """The OpenAI TextEmbedder will batch across rows, and split a large row into a batch request when necessary.

    Note:
        This limits us to 300k tokens per row which is a reasonable start.
        This implementation also uses len(text)*5 to estimate token count
        which is conservative and O(1) rather than being perfect with tiktoken.
    """

    _client: AsyncOpenAI
    _model: str
    _dimensions: int | None

    def __init__(
        self,
        provider_options: OpenAIProviderOptions,
        model: str,
        embed_options: EmbedTextOptions,
        dimensions: int | None = None,
        zero_on_failure: bool = False,
        provider_name: str = "openai",
    ):
        self._model = model
        self._zero_on_failure = zero_on_failure
        self._dimensions = dimensions
        self._provider_name = provider_name

        merged_provider_options: dict[str, Any] = merge_provider_and_api_options(
            provider_options=provider_options,
            api_options=embed_options,
            provider_option_type=OpenAIProviderOptions,
        )

        self._client = AsyncOpenAI(**merged_provider_options)

    async def embed_text(self, text: list[str]) -> list[Embedding]:
        embeddings: list[Embedding] = []
        curr_batch: list[str] = []
        curr_batch_token_count: int = 0

        batch_token_limit = 300_000
        approx_chars_per_token = 3  # round down for conservative estimate of "1 token ≈ 4 characters"
        input_text_token_limit = 8192
        input_text_chars_limit = input_text_token_limit * approx_chars_per_token

        async def flush() -> None:
            nonlocal curr_batch
            nonlocal curr_batch_token_count
            if len(curr_batch) == 0:
                return None
            embeddings_result = await self._embed_text_batch(curr_batch)
            embeddings.extend(embeddings_result)
            curr_batch = []
            curr_batch_token_count = 0

        for input_text in text:
            # Handle None values by treating them as empty strings
            if input_text is None:
                input_text = ""
            input_text_token_count = len(input_text) // approx_chars_per_token
            if input_text_token_count > input_text_token_limit:
                # Must process previous inputs first, if any, to maintain ordered outputs.
                await flush()
                # If the current input exceeds the maximum tokens per input (8192),
                # then we will split this single input into its own batch request.
                chunked_batch = chunk_text(input_text, input_text_chars_limit)
                chunked_result = await self._embed_text_batch(chunked_batch)
                # We combine all result embedding vectors into a single embedding using a weighted average.
                # https://github.com/openai/openai-cookbook/blob/main/examples/Embedding_long_inputs.ipynb
                chunked_lens = [len(chunk) for chunk in chunked_batch]
                chunked_vec = np.average(chunked_result, axis=0, weights=chunked_lens)
                chunked_vec = chunked_vec / np.linalg.norm(chunked_vec)  # normalizes length to 1
                embeddings.append(chunked_vec)
            elif input_text_token_count + curr_batch_token_count >= batch_token_limit:
                await flush()
                curr_batch.append(input_text)
                curr_batch_token_count += input_text_token_count
            else:
                curr_batch.append(input_text)
                curr_batch_token_count += input_text_token_count
        await flush()

        return embeddings

    async def _embed_text_batch(self, input_batch: list[str]) -> list[Embedding]:
        """Embeds text as a batch call, falling back to _embed_text on rate limit exceptions."""
        try:
            response = await self._client.embeddings.create(
                input=input_batch,
                model=self._model,
                encoding_format="float",
                dimensions=self._dimensions or NOT_GIVEN,
            )
            self._record_usage_metrics(response)
            return [np.array(embedding.embedding) for embedding in response.data]
        except RateLimitError:
            # fall back to individual calls when rate limited
            # consider sleeping or other backoff mechanisms
            return await asyncio.gather(*(self._embed_text(text) for text in input_batch))
        except OpenAIError as ex:
            raise ValueError("The `embed_text` method encountered an OpenAI error.") from ex

    async def _embed_text(self, input_text: str) -> Embedding:
        """Embeds a single text input and possibly returns a zero vector."""
        try:
            response: CreateEmbeddingResponse = await self._client.embeddings.create(
                input=input_text,
                model=self._model,
                encoding_format="float",
                dimensions=self._dimensions or NOT_GIVEN,
            )
            self._record_usage_metrics(response)
            return np.array(response.data[0].embedding)
        except Exception as ex:
            if self._zero_on_failure:
                size = self._dimensions or _models[self._model].dimensions.size
                return np.zeros(size, dtype=np.float32)
            else:
                raise ex

    def _record_usage_metrics(self, response: CreateEmbeddingResponse) -> None:
        usage = getattr(response, "usage", None)
        if usage is None or not isinstance(usage, Usage):
            return

        input_tokens = usage.prompt_tokens
        total_tokens = usage.total_tokens

        record_token_metrics(
            protocol="embed",
            model=self._model,
            provider=self._provider_name,
            input_tokens=input_tokens,
            total_tokens=total_tokens,
        )


def chunk_text(text: str, size: int) -> list[str]:
    return [text[i : i + size] for i in range(0, len(text), size)]
