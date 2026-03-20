from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from google import genai
from google.genai import errors as genai_errors
from google.genai import types

from daft import DataType
from daft.ai.google.typing import GoogleProviderOptions
from daft.ai.metrics import record_token_metrics
from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor
from daft.ai.typing import EmbeddingDimensions, EmbedTextOptions, Options, UDFOptions
from daft.ai.utils import merge_provider_and_api_options
from daft.dependencies import np

if TYPE_CHECKING:
    from daft.ai.typing import Embedding


_DEFAULT_TOKEN_LIMIT = 2048


@dataclass(frozen=True)
class _GoogleModelProfile:
    """Model profiles contain various model-specific metadata."""

    dimensions: EmbeddingDimensions
    supports_overriding_dimensions: bool
    input_text_token_limit: int = _DEFAULT_TOKEN_LIMIT
    min_dimensions: int | None = None
    max_dimensions: int | None = None


_models: dict[str, _GoogleModelProfile] = {
    "gemini-embedding-001": _GoogleModelProfile(
        dimensions=EmbeddingDimensions(
            size=3072,  # default for gemini-embedding-001
            dtype=DataType.float32(),
        ),
        supports_overriding_dimensions=True,
        min_dimensions=128,
        max_dimensions=3072,
    ),
    "gemini-embedding-2-preview": _GoogleModelProfile(
        dimensions=EmbeddingDimensions(
            size=3072,  # default for gemini-embedding-2-preview
            dtype=DataType.float32(),
        ),
        supports_overriding_dimensions=True,
        input_text_token_limit=8192,
        min_dimensions=128,
        max_dimensions=3072,
    ),
}


def get_input_text_token_limit_for_model(model_name: str) -> int:
    """Get the input token limit for a model, with fallback to default.

    Args:
        model_name: The name of the embedding model.

    Returns:
        The input text token limit for the model. Returns 2048 for unknown models.
    """
    if model_name in _models:
        return _models[model_name].input_text_token_limit
    else:
        return _DEFAULT_TOKEN_LIMIT


@dataclass
class GoogleTextEmbedderDescriptor(TextEmbedderDescriptor):
    provider_name: str
    provider_options: GoogleProviderOptions
    model_name: str
    dimensions: int | None
    embed_options: EmbedTextOptions = field(
        default_factory=lambda: EmbedTextOptions(batch_size=64, max_retries=3, on_error="raise")
    )

    def __post_init__(self) -> None:
        if self.model_name in _models:
            model = _models[self.model_name]
            if self.dimensions is not None:
                if not model.supports_overriding_dimensions:
                    raise ValueError(
                        f"Google embedding model '{self.model_name}' does not support specifying dimensions"
                    )
                if model.min_dimensions is not None and self.dimensions < model.min_dimensions:
                    raise ValueError(
                        f"dimensions={self.dimensions} is below the minimum of {model.min_dimensions} "
                        f"for model '{self.model_name}'"
                    )
                if model.max_dimensions is not None and self.dimensions > model.max_dimensions:
                    raise ValueError(
                        f"dimensions={self.dimensions} exceeds the maximum of {model.max_dimensions} "
                        f"for model '{self.model_name}'"
                    )
                if "supports_overriding_dimensions" not in self.embed_options:
                    self.embed_options["supports_overriding_dimensions"] = True

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return dict(self.embed_options)

    def get_dimensions(self) -> EmbeddingDimensions:
        if self.dimensions is not None:
            if self.model_name in _models:
                return EmbeddingDimensions(size=self.dimensions, dtype=_models[self.model_name].dimensions.dtype)
            return EmbeddingDimensions(size=self.dimensions, dtype=DataType.float32())

        if self.model_name in _models:
            return _models[self.model_name].dimensions

        # For unknown models, probe to determine dimensions
        try:
            merged_provider_options: dict[str, Any] = merge_provider_and_api_options(
                provider_options=self.provider_options,
                api_options=self.embed_options,
                provider_option_type=GoogleProviderOptions,
            )
            client = genai.Client(**merged_provider_options)
            response = client.models.embed_content(
                model=self.model_name,
                contents=["dimension probe"],
            )
            if response.embeddings and response.embeddings[0].values:
                size = len(response.embeddings[0].values)
                return EmbeddingDimensions(size=size, dtype=DataType.float32())
            raise ValueError("Empty embedding response during dimension probe.")
        except Exception as ex:
            raise ValueError(
                "Failed to determine embedding dimensions from Google embedding model. "
                "Specify `dimensions=...` or use a known model."
            ) from ex

    def get_udf_options(self) -> UDFOptions:
        options = super().get_udf_options()
        options.max_retries = 0  # Google GenAI client handles retries internally
        return options

    def is_async(self) -> bool:
        return True

    def instantiate(self) -> TextEmbedder:
        input_text_token_limit = get_input_text_token_limit_for_model(self.model_name)
        batch_token_limit = self.embed_options.get("batch_token_limit", input_text_token_limit)

        return GoogleTextEmbedder(
            provider_options=self.provider_options,
            model=self.model_name,
            embed_options=self.embed_options,
            dimensions=self.dimensions if self.embed_options.get("supports_overriding_dimensions", False) else None,
            provider_name=self.provider_name,
            batch_token_limit=batch_token_limit,
            input_text_token_limit=input_text_token_limit,
        )


class GoogleTextEmbedder(TextEmbedder):
    """Google GenAI TextEmbedder using the google-genai SDK.

    Batches across rows, and splits large rows into chunked batch requests
    when necessary. Uses len(text) // 3 to estimate token count (conservative O(1)).
    """

    _client: genai.Client
    _model: str
    _dimensions: int | None
    _batch_token_limit: int
    _input_text_token_limit: int
    _embed_config: types.EmbedContentConfig

    def __init__(
        self,
        provider_options: GoogleProviderOptions,
        model: str,
        embed_options: EmbedTextOptions,
        dimensions: int | None = None,
        zero_on_failure: bool = False,
        provider_name: str = "google",
        batch_token_limit: int | None = None,
        input_text_token_limit: int = _DEFAULT_TOKEN_LIMIT,
    ):
        self._model = model
        self._zero_on_failure = zero_on_failure
        self._dimensions = dimensions
        self._provider_name = provider_name
        self._input_text_token_limit = input_text_token_limit
        self._batch_token_limit = input_text_token_limit if batch_token_limit is None else batch_token_limit

        merged_provider_options: dict[str, Any] = merge_provider_and_api_options(
            provider_options=provider_options,
            api_options=embed_options,
            provider_option_type=GoogleProviderOptions,
        )

        embed_config_keys = types.EmbedContentConfig.model_fields.keys()
        embed_config_params = {key: value for key, value in embed_options.items() if key in embed_config_keys}
        if self._dimensions is not None:
            embed_config_params["output_dimensionality"] = self._dimensions

        self._embed_config = types.EmbedContentConfig(**embed_config_params)
        self._client = genai.Client(**merged_provider_options)

    def _should_normalize_output(self) -> bool:
        if self._model not in _models:
            return False
        output_dimensions = self._dimensions if self._dimensions is not None else _models[self._model].dimensions.size
        # Google recommends normalizing embeddings when output_dimensionality is
        # reduced below the default 3072 (the full 3072-dimensional output
        # is already normalized by the model)
        return output_dimensions < 3072

    def _normalize_embedding(self, embedding: Embedding) -> Embedding:
        norm = np.linalg.norm(embedding)
        if norm == 0:
            return embedding
        return embedding / norm

    async def embed_text(self, text: list[str]) -> list[Embedding]:
        embeddings: list[Embedding] = []
        curr_batch: list[str] = []
        curr_batch_token_count: int = 0

        approx_chars_per_token = 3
        input_text_chars_limit = self._input_text_token_limit * approx_chars_per_token

        async def flush_batch(batch: list[str]) -> list[Embedding]:
            if len(batch) == 0:
                return []
            return await self._embed_text_batch(batch)

        for input_text in text:
            if input_text is None:
                input_text = ""
            input_text_token_count = len(input_text) // approx_chars_per_token
            if input_text_token_count > self._input_text_token_limit:
                embeddings.extend(await flush_batch(curr_batch))
                curr_batch = []
                curr_batch_token_count = 0
                chunked_batch = chunk_text(input_text, input_text_chars_limit)
                chunked_result = await self._embed_text_batch(chunked_batch, normalize_output=False)
                chunked_lens = [len(chunk) for chunk in chunked_batch]
                chunked_vec = np.average(chunked_result, axis=0, weights=chunked_lens)
                chunked_vec = self._normalize_embedding(chunked_vec)
                embeddings.append(chunked_vec)
            elif input_text_token_count + curr_batch_token_count >= self._batch_token_limit:
                embeddings.extend(await flush_batch(curr_batch))
                curr_batch = [input_text]
                curr_batch_token_count = input_text_token_count
            else:
                curr_batch.append(input_text)
                curr_batch_token_count += input_text_token_count
        embeddings.extend(await flush_batch(curr_batch))

        return embeddings

    async def _embed_text_batch(self, input_batch: list[str], normalize_output: bool = True) -> list[Embedding]:
        """Embeds text as a batch call, falling back to individual calls on rate limit."""
        try:
            should_normalize = normalize_output and self._should_normalize_output()
            response = await self._client.aio.models.embed_content(
                model=self._model,
                contents=input_batch,
                config=self._embed_config,
            )
            self._record_usage_metrics(response)
            if response.embeddings is None:
                raise ValueError("Google embedding API returned no embeddings.")
            embeddings = [np.array(emb.values, dtype=np.float32) for emb in response.embeddings]
            if not should_normalize:
                return embeddings
            return [self._normalize_embedding(embedding) for embedding in embeddings]
        except genai_errors.APIError as exc:
            if exc.code == 429:
                return await asyncio.gather(
                    *(self._embed_text(t, normalize_output=normalize_output) for t in input_batch)
                )
            raise ValueError("The `embed_text` method encountered a Google API error.") from exc

    async def _embed_text(self, input_text: str, normalize_output: bool = True) -> Embedding:
        """Embeds a single text input and possibly returns a zero vector on failure."""
        try:
            should_normalize = normalize_output and self._should_normalize_output()
            response = await self._client.aio.models.embed_content(
                model=self._model,
                contents=[input_text],
                config=self._embed_config,
            )
            self._record_usage_metrics(response)
            if response.embeddings is None or len(response.embeddings) == 0:
                raise ValueError("Google embedding API returned no embeddings.")
            embedding = np.array(response.embeddings[0].values, dtype=np.float32)
            if not should_normalize:
                return embedding
            return self._normalize_embedding(embedding)
        except Exception as ex:
            if self._zero_on_failure:
                model_profile = _models.get(self._model)
                if model_profile:
                    dtype = model_profile.dimensions.dtype
                    size = self._dimensions or model_profile.dimensions.size
                else:
                    dtype = DataType.float32()
                    size = self._dimensions or 768
                np_dtype = np.float64 if dtype == DataType.float64() else np.float32
                return np.zeros(size, dtype=np_dtype)
            else:
                raise ex

    def _record_usage_metrics(self, response: types.EmbedContentResponse) -> None:
        metadata = response.metadata
        if metadata is None:
            return

        billable_chars = metadata.billable_character_count or 0
        record_token_metrics(
            protocol="embed",
            model=self._model,
            provider=self._provider_name,
            input_tokens=billable_chars,
            total_tokens=billable_chars,
        )


def chunk_text(text: str, size: int) -> list[str]:
    return [text[i : i + size] for i in range(0, len(text), size)]
