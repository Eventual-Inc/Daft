from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from daft.ai.typing import Descriptor

if TYPE_CHECKING:
    from daft.ai.typing import Embedding, EmbeddingDimensions, Image, Label


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


@runtime_checkable
class ImageEmbedder(Protocol):
    """Protocol for image embedding implementations."""

    def embed_image(self, images: list[Image]) -> list[Embedding]:
        """Embeds a batch of images into an embedding vector."""
        ...


class ImageEmbedderDescriptor(Descriptor[ImageEmbedder]):
    """Descriptor for an ImageEmbedder implementation."""

    @abstractmethod
    def get_dimensions(self) -> EmbeddingDimensions:
        """Returns the dimensions of the embeddings produced by the described ImageEmbedder."""


@runtime_checkable
class TextClassifier(Protocol):
    """Protocol for text classification implementations."""

    def classify_text(self, text: list[str], labels: Label | list[Label]) -> list[Label]:
        """Classifies a batch of text strings using the given label(s)."""
        ...


class TextClassifierDescriptor(Descriptor[TextClassifier]):
    """Descriptor for a TextClassifier implementation."""
