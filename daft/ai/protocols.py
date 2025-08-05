from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, abstractmethod

if TYPE_CHECKING:
    from daft.ai.typing import Embedding, EmbeddingDimensions


class TextEmbedder(ABC):
    """Interface for text embedding implementations."""

    @abstractmethod
    def get_dimensions(self) -> EmbeddingDimensions:
        """Returns the embedding dimensions produced by this embedding model implementation."""
        ...

    @abstractmethod
    def embed_text(self, text: list[str]) -> list[Embedding]:
        """Embeds text string into an embedding vector."""
        ...
