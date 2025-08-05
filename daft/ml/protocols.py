from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from daft.ml.model import Model

if TYPE_CHECKING:
    from daft.ml.typing import Embedding, EmbeddingDimensions


##
# EMBEDDER PROTOCOLS
##


@runtime_checkable
class TextEmbedder(Model, Protocol):
    """Protocol for a text embedding model that processes single strings."""

    @property
    def dimensions(self) -> EmbeddingDimensions:
        """Returns the embedding dimensions produced by this embedding model implementation."""
        ...

    def embed_text(self, text: str) -> Embedding:
        """Embeds a single text string into an embedding vector."""
        ...


@runtime_checkable
class TextEmbedderBatched(Model, Protocol):
    """Protocol for a text embedding model that processes single strings."""

    @property
    def dimensions(self) -> EmbeddingDimensions:
        """Returns the embedding dimensions produced by this embedding model implementation."""
        ...

    def embed_text(self, text: list[str]) -> list[Embedding]:
        """Embeds a batch of text strings into an embedding vector batch."""
        ...


TextEmbedderLike = TextEmbedder | TextEmbedderBatched
