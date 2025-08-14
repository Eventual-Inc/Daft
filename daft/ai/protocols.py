from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from daft.ai.typing import Descriptor

if TYPE_CHECKING:
    from daft.ai.typing import Embedding, EmbeddingDimensions


@runtime_checkable
class TextEmbedder(Protocol):
    """Protocol for text embedding implementations."""

    def embed_text(self, text: list[str]) -> list[Embedding]:
        """Embeds a batch of text strings into an embedding vector."""
        ...


class TextEmbedderDescriptor(Descriptor[TextEmbedder]):
    """Descriptor for a TextEmbedder implementation."""

    @abstractmethod
    def get_dimensions(self) -> EmbeddingDimensions:
        """Returns the dimensions of the embeddings produced by the described TextEmbedder."""
