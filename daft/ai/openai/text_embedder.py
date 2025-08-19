from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from openai import OpenAI

from daft import DataType
from daft.ai.protocols import TextEmbedder, TextEmbedderDescriptor
from daft.ai.typing import EmbeddingDimensions, Options

if TYPE_CHECKING:
    from daft.ai.openai.typing import OpenAIProviderOptions
    from daft.ai.typing import Embedding


@dataclass(frozen=True)
class _Profile:
    """Model profiles contain various model-specific metadata.

    Note:
        This is a bit simpler than OO-inheritance to model the subtle
        differences between the models. If there is a need for different
        implementations, then it would make sense to
    """

    dimensions: EmbeddingDimensions


_profiles: dict[str, _Profile] = {
    "text-embedding-ada-002": _Profile(
        dimensions=EmbeddingDimensions(size=1536, dtype=DataType.float32()),
    ),
    "text-embedding-3-small": _Profile(
        dimensions=EmbeddingDimensions(size=1536, dtype=DataType.float32()),
    ),
    "text-embedding-3-large": _Profile(
        dimensions=EmbeddingDimensions(size=3072, dtype=DataType.float32()),
    ),
}


@dataclass
class OpenAITextEmbedderDescriptor(TextEmbedderDescriptor):
    provider_name: str
    provider_options: OpenAIProviderOptions
    model_name: str
    model_options: Options

    def get_provider(self) -> str:
        return "openai"

    def get_model(self) -> str:
        return self.model_name

    def get_options(self) -> Options:
        return self.model_options

    def get_dimensions(self) -> EmbeddingDimensions:
        return _profiles[self.model_name].dimensions

    def instantiate(self) -> TextEmbedder:
        return OpenAITextEmbedder(
            client=OpenAI(**self.provider_options),
            model=self.model_name,
        )


class OpenAITextEmbedder(TextEmbedder):
    _client: OpenAI
    _model: str

    def __init__(self, client: OpenAI, model: str):
        self._client = client
        self._model = model

    def embed_text(self, text: list[str]) -> list[Embedding]:
        # OpenAI has a limit of 300,000 tokens per request and 8192 tokens per input
        # We'll process in batches to handle large inputs
        batch_size = 100  # Conservative batch size
        all_embeddings = []

        for i in range(0, len(text), batch_size):
            batch_text = text[i : i + batch_size]

            try:
                response = self.client.embeddings.create(model=self.model, input=batch_text, encoding_format="float")

                # Extract embeddings from response
                batch_embeddings = [embedding.embedding for embedding in response.data]
                all_embeddings.extend(batch_embeddings)

            except Exception:
                # If batch fails, try processing one by one
                for single_text in batch_text:
                    try:
                        response = self.client.embeddings.create(
                            model=self.model, input=[single_text], encoding_format="float"
                        )
                        all_embeddings.append(response.data[0].embedding)
                    except Exception:
                        # dim = _profiles[self.]

                        # If individual text fails, add a zero vector as fallback
                        # Use the expected dimensions for the model
                        if "ada-002" in self.model or "3-small" in self.model:
                            fallback_dim = 1536
                        elif "3-large" in self.model:
                            fallback_dim = 3072
                        else:
                            fallback_dim = 1536

                        import numpy as np

                        all_embeddings.append(np.zeros(fallback_dim, dtype=np.float32))

        return all_embeddings
