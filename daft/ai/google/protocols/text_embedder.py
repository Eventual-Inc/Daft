from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from google import genai
from google.genai import errors as genai_errors
from google.genai import types

from daft import DataType
from daft.ai.google.typing import GoogleProviderOptions
from daft.ai.metrics import record_text_embedding_metrics
from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor
from daft.ai.typing import EmbeddingDimensions, EmbeddingModelProfile, EmbedTextOptions, Options
from daft.ai.utils import merge_provider_and_api_options, raise_retry_after_from_response
from daft.dependencies import np

if TYPE_CHECKING:
    from daft.ai.typing import Embedding


# Google embedding models
# See: https://ai.google.dev/gemini-api/docs/embeddings
_models: dict[str, EmbeddingModelProfile] = {
    "gemini-embedding-001": EmbeddingModelProfile(
        dimensions=EmbeddingDimensions(
            size=3072,
            dtype=DataType.float32(),
        ),
        supports_overriding_dimensions=True,
    ),
}

# Google API hard limits for Gemini embeddings
_BATCH_SIZE_LIMIT = 100
_INPUT_TOKEN_LIMIT = 2048


@dataclass
class GoogleTextEmbedderDescriptor(TextEmbedderDescriptor):
    provider_name: str
    provider_options: GoogleProviderOptions
    model_name: str
    dimensions: int | None
    embed_options: EmbedTextOptions = field(
        default_factory=lambda: EmbedTextOptions(batch_size=100, max_retries=3, on_error="raise")
    )

    def __post_init__(self) -> None:
        # Validate model if known
        if self.model_name in _models:
            model = _models[self.model_name]
            if self.dimensions is not None and not model.supports_overriding_dimensions:
                raise ValueError(f"Google embedding model '{self.model_name}' does not support specifying dimensions")

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return dict(self.embed_options)

    def get_dimensions(self) -> EmbeddingDimensions:
        # If user specified dimensions, use that
        if self.dimensions is not None:
            return EmbeddingDimensions(size=self.dimensions, dtype=DataType.float32())
        # If model is known, use its default dimensions
        if self.model_name in _models:
            return _models[self.model_name].dimensions
        # Fallback for unknown models (use gemini-embedding-001 default)
        return EmbeddingDimensions(size=3072, dtype=DataType.float32())

    def is_async(self) -> bool:
        return True

    def instantiate(self) -> TextEmbedder:
        return GoogleTextEmbedder(
            provider_name=self.provider_name,
            provider_options=self.provider_options,
            model=self.model_name,
            embed_options=self.embed_options,
            dimensions=self.dimensions,
        )


class GoogleTextEmbedder(TextEmbedder):
    """Google TextEmbedder will batch across rows, and split a large row into multiple embed requests when necessary.

    Mirrors the OpenAI embedder behavior:
    - batches inputs to reduce API calls
    - splits long single inputs into chunks and returns a single combined embedding
      via a length-weighted average + L2 normalization
    - uses an O(1) token estimate based on character count
    """

    def __init__(
        self,
        provider_name: str,
        provider_options: GoogleProviderOptions,
        model: str,
        embed_options: EmbedTextOptions,
        dimensions: int | None = None,
    ) -> None:
        self.provider_name = provider_name
        self.model = model
        self.dimensions = dimensions
        self._embed_options = dict(embed_options)

        merged_provider_options = merge_provider_and_api_options(
            provider_options=provider_options,
            api_options=embed_options,
            provider_option_type=GoogleProviderOptions,
        )

        # Prepare embedding config
        embed_config: dict[str, Any] = {}
        if self.dimensions is not None:
            embed_config["output_dimensionality"] = self.dimensions

        embed_config_keys = types.EmbedContentConfig.model_fields.keys()
        for key, value in embed_options.items():
            if key in embed_config_keys:
                embed_config[key] = value

        self._embed_content_config = types.EmbedContentConfig(**embed_config)

        self._client = genai.Client(**merged_provider_options)

        requested_batch_size = int(embed_options.get("batch_size", _BATCH_SIZE_LIMIT) or _BATCH_SIZE_LIMIT)
        self._batch_size = max(1, min(_BATCH_SIZE_LIMIT, requested_batch_size))

    async def embed_text(self, text: list[str]) -> list[Embedding]:
        if not text:
            return []

        embeddings: list[Embedding] = []
        curr_batch: list[str] = []
        curr_batch_token_count: int = 0

        # Conservative, O(1) token estimate (mirrors OpenAI embedder)
        approx_chars_per_token = 3
        input_text_token_limit = _INPUT_TOKEN_LIMIT
        input_text_chars_limit = input_text_token_limit * approx_chars_per_token
        batch_token_limit = self._batch_size * input_text_token_limit

        async def flush() -> None:
            nonlocal curr_batch, curr_batch_token_count
            if not curr_batch:
                return
            embeddings_result = await self._embed_text_batch(curr_batch)
            embeddings.extend(embeddings_result)
            curr_batch = []
            curr_batch_token_count = 0

        for input_text in text:
            if input_text is None:
                input_text = ""

            input_text_token_count = len(input_text) // approx_chars_per_token

            if input_text_token_count > input_text_token_limit:
                # Must process previous inputs first, if any, to maintain ordered outputs.
                await flush()
                chunked_batch = chunk_text(input_text, input_text_chars_limit)
                chunked_result = await self._embed_text_batch(chunked_batch)
                chunked_lens = [len(chunk) for chunk in chunked_batch]
                chunked_vec = np.average(chunked_result, axis=0, weights=chunked_lens)
                norm = np.linalg.norm(chunked_vec)
                if norm != 0:
                    chunked_vec = chunked_vec / norm
                embeddings.append(chunked_vec)
                continue

            would_exceed_count = len(curr_batch) + 1 > self._batch_size
            would_exceed_tokens = input_text_token_count + curr_batch_token_count >= batch_token_limit
            if would_exceed_count or would_exceed_tokens:
                await flush()

            curr_batch.append(input_text)
            curr_batch_token_count += input_text_token_count

        await flush()
        return embeddings

    async def _embed_text_batch(self, input_batch: list[str]) -> list[Embedding]:
        results: list[Embedding] = []
        for i in range(0, len(input_batch), self._batch_size):
            sub_batch = input_batch[i : i + self._batch_size]
            try:
                response = await self._client.aio.models.embed_content(
                    model=self.model,
                    contents=sub_batch,
                    config=self._embed_content_config,
                )
            except genai_errors.APIError as exc:
                if exc.code == 429 or exc.code == 503:
                    raise_retry_after_from_response(exc.response, exc)
                raise ValueError("The `embed_text` method encountered a Google GenAI API error.") from exc
            except Exception as exc:
                raise ValueError(f"The `embed_text` method encountered a Google GenAI error: {exc}") from exc

            embeddings_obj = getattr(response, "embeddings", None)
            if embeddings_obj is None:
                raise ValueError("Google GenAI embed response missing `embeddings` field.")

            # Metrics:
            # - Gemini API responses generally do not include usage metadata.
            # - Vertex AI responses may include per-embedding statistics.token_count.
            usage = getattr(response, "usage_metadata", None)
            if usage is not None:
                input_tokens = getattr(usage, "prompt_token_count", None)
                total_tokens = getattr(usage, "total_token_count", None)
                record_text_embedding_metrics(
                    model=self.model,
                    provider=self.provider_name,
                    num_texts=len(sub_batch),
                    input_tokens=(input_tokens or 0) if input_tokens is not None else None,
                    total_tokens=(total_tokens or 0) if total_tokens is not None else None,
                )
            else:
                token_counts: list[float] = []
                for emb in embeddings_obj:
                    stats = getattr(emb, "statistics", None)
                    if stats is None:
                        continue
                    token_count = getattr(stats, "token_count", None)
                    if token_count is not None:
                        try:
                            token_counts.append(float(token_count))
                        except (TypeError, ValueError):
                            # Non-Vertex responses (and some test mocks) may not have numeric token counts.
                            continue
                record_text_embedding_metrics(
                    model=self.model,
                    provider=self.provider_name,
                    num_texts=len(sub_batch),
                    input_tokens=int(sum(token_counts)) if token_counts else None,
                )

            expected_dim = self.dimensions
            if expected_dim is None:
                expected_dim = _models.get(self.model, _models["gemini-embedding-001"]).dimensions.size
            should_normalize = expected_dim != 3072

            for emb in embeddings_obj:
                vec = np.array(emb.values, dtype=np.float32)
                if should_normalize:
                    norm = np.linalg.norm(vec)
                    if norm != 0:
                        vec = vec / norm
                results.append(vec)

        return results


def chunk_text(text: str, size: int) -> list[str]:
    return [text[i : i + size] for i in range(0, len(text), size)]
