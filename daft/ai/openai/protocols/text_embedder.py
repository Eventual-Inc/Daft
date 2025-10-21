from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from openai import OpenAI, OpenAIError, RateLimitError

from daft import DataType
from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor
from daft.ai.typing import EmbeddingDimensions, Options, UDFOptions
from daft.ai.utils import get_http_udf_options
from daft.dependencies import np

if TYPE_CHECKING:
    from openai.types import EmbeddingModel
    from openai.types.create_embedding_response import CreateEmbeddingResponse

    from daft.ai.openai.typing import OpenAIProviderOptions
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


_models: dict[EmbeddingModel, _ModelProfile] = {
    "text-embedding-ada-002": _ModelProfile(
        dimensions=EmbeddingDimensions(
            size=1536,
            dtype=DataType.float32(),
        ),
    ),
    "text-embedding-3-small": _ModelProfile(
        dimensions=EmbeddingDimensions(
            size=1536,
            dtype=DataType.float32(),
        ),
    ),
    "text-embedding-3-large": _ModelProfile(
        dimensions=EmbeddingDimensions(
            size=3072,
            dtype=DataType.float32(),
        ),
    ),
}


@dataclass
class OpenAITextEmbedderDescriptor(TextEmbedderDescriptor):
    provider_name: str
    provider_options: OpenAIProviderOptions
    model_name: str
    model_options: Options

    def __post_init__(self) -> None:
        if self.model_name not in _models:
            supported_models = ", ".join(_models.keys())
            raise ValueError(
                f"Unsupported OpenAI embedding model '{self.model_name}', expected one of: {supported_models}"
            )

    def get_provider(self) -> str:
        return self.provider_name

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return self.model_options

    def get_dimensions(self) -> EmbeddingDimensions:
        return _models[self.model_name].dimensions

    def get_udf_options(self) -> UDFOptions:
        return get_http_udf_options()

    def instantiate(self) -> TextEmbedder:
        return OpenAITextEmbedder(
            client=OpenAI(**self.provider_options),
            model=self.model_name,
        )


@dataclass
class LMStudioTextEmbedderDescriptor(TextEmbedderDescriptor):
    """LM Studio text embedder descriptor that dynamically discovers model dimensions.

    Unlike OpenAI, LM Studio can load different models with varying embedding dimensions.
    This descriptor queries the local server to get the actual model dimensions.
    """

    provider_name: str
    provider_options: OpenAIProviderOptions
    model_name: str
    model_options: Options

    def get_provider(self) -> str:
        return "lm_studio"

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return self.model_options

    def get_dimensions(self) -> EmbeddingDimensions:
        try:
            client = OpenAI(**self.provider_options)
            response = client.embeddings.create(
                input="dimension probe",
                model=self.model_name,
                encoding_format="float",
            )
            size = len(response.data[0].embedding)
            return EmbeddingDimensions(size=size, dtype=DataType.float32())
        except Exception as ex:
            raise ValueError("Failed to determine embedding dimensions from LM Studio.") from ex

    def get_udf_options(self) -> UDFOptions:
        return get_http_udf_options()

    def instantiate(self) -> TextEmbedder:
        return OpenAITextEmbedder(
            client=OpenAI(**self.provider_options),
            model=self.model_name,
        )


class OpenAITextEmbedder(TextEmbedder):
    """The OpenAI TextEmbedder will batch across rows, and split a large row into a batch request when necessary.

    Note:
        This limits us to 300k tokens per row which is a reasonable start.
        This implementation also uses len(text)*5 to estimate token count
        which is conservative and O(1) rather than being perfect with tiktoken.
    """

    _client: OpenAI
    _model: str

    def __init__(self, client: OpenAI, model: str, zero_on_failure: bool = False):
        self._client = client
        self._model = model
        self._zero_on_failure = zero_on_failure

    def embed_text(self, text: list[str]) -> list[Embedding]:
        embeddings: list[Embedding] = []
        curr_batch: list[str] = []
        curr_batch_token_count: int = 0

        batch_token_limit = 300_000
        approx_chars_per_token = 3  # round down for conservative estimate of "1 token ≈ 4 characters"
        input_text_token_limit = 8192
        input_text_chars_limit = input_text_token_limit * approx_chars_per_token

        def flush() -> None:
            nonlocal curr_batch
            nonlocal curr_batch_token_count
            if len(curr_batch) == 0:
                return None
            embeddings_result = self._embed_text_batch(curr_batch)
            embeddings.extend(embeddings_result)
            curr_batch = []
            curr_batch_token_count = 0

        for input_text in text:
            input_text_token_count = len(input_text) // approx_chars_per_token
            if input_text_token_count > input_text_token_limit:
                # Must process previous inputs first, if any, to maintain ordered outputs.
                flush()
                # If the current input exceeds the maximum tokens per input (8192),
                # then we will split this single input into its own batch request.
                chunked_batch = chunk_text(input_text, input_text_chars_limit)
                chunked_result = self._embed_text_batch(chunked_batch)
                # We combine all result embedding vectors into a single embedding using a weighted average.
                # https://github.com/openai/openai-cookbook/blob/main/examples/Embedding_long_inputs.ipynb
                chunked_lens = [len(chunk) for chunk in chunked_batch]
                chunked_vec = np.average(chunked_result, axis=0, weights=chunked_lens)
                chunked_vec = chunked_vec / np.linalg.norm(chunked_vec)  # normalizes length to 1
                embeddings.append(chunked_vec)
            elif input_text_token_count + curr_batch_token_count >= batch_token_limit:
                flush()
            else:
                curr_batch.append(input_text)
                curr_batch_token_count += input_text_token_count
        flush()

        return embeddings

    def _embed_text_batch(self, input_batch: list[str]) -> list[Embedding]:
        """Embeds text as a batch call, falling back to _embed_text on rate limit exceptions."""
        try:
            response = self._client.embeddings.create(
                input=input_batch,
                model=self._model,
                encoding_format="float",
            )
            return [np.array(embedding.embedding) for embedding in response.data]
        except RateLimitError:
            # fall back to individual calls when rate limited
            # consider sleeping or other backoff mechanisms
            return [self._embed_text(text) for text in input_batch]
        except OpenAIError as ex:
            raise ValueError("The `embed_text` method encountered an OpenAI error.") from ex

    def _embed_text(self, input_text: str) -> Embedding:
        """Embeds a single text input and possibly returns a zero vector."""
        try:
            response: CreateEmbeddingResponse = self._client.embeddings.create(
                input=input_text,
                model=self._model,
                encoding_format="float",
            )
            return np.array(response.data[0].embedding)
        except Exception as ex:
            if self._zero_on_failure:
                size = _models[self._model].dimensions.size
                return np.zeros(size, dtype=np.float32)
            else:
                raise ex


def chunk_text(text: str, size: int) -> list[str]:
    return [text[i : i + size] for i in range(0, len(text), size)]
