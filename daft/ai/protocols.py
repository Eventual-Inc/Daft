from __future__ import annotations

import sys
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Protocol, TypedDict, runtime_checkable

if sys.version_info < (3, 11):
    from typing_extensions import NotRequired
else:
    from typing import NotRequired

from daft.ai.typing import Descriptor, EmbeddingDimensions, EmbedTextOptions

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from daft.ai.typing import Embedding, Image, Label


@runtime_checkable
class TextEmbedder(Protocol):
    """Protocol for text embedding implementations."""

    def embed_text(self, text: list[str]) -> list[Embedding] | Awaitable[list[Embedding]]:
        """Embeds a batch of text strings into an embedding vector."""


class TextEmbedderDescriptor(TypedDict):
    """Serializable descriptor for a text embedder. No logic — just data.

    The ``provider`` field is required for resolution on the other end of serde.
    """

    provider: str
    model: str
    dimensions: EmbeddingDimensions
    embed_options: NotRequired[EmbedTextOptions]


@runtime_checkable
class ImageEmbedder(Protocol):
    """Protocol for image embedding implementations."""

    def embed_image(self, images: list[Image]) -> list[Embedding] | Awaitable[list[Embedding]]:
        """Embeds a batch of images into an embedding vector."""
        ...


class ImageEmbedderDescriptor(Descriptor[ImageEmbedder]):
    """Descriptor for an ImageEmbedder implementation."""

    @abstractmethod
    def get_dimensions(self) -> EmbeddingDimensions:
        """Returns the dimensions of the embeddings produced by the described ImageEmbedder."""

    def is_async(self) -> bool:
        """Whether the described ImageEmbedder produces awaitable results."""
        return False


@runtime_checkable
class TextClassifier(Protocol):
    """Protocol for text classification implementations."""

    def classify_text(self, text: list[str], labels: Label | list[Label]) -> list[Label]:
        """Classifies a batch of text strings using the given label(s)."""
        ...


class TextClassifierDescriptor(Descriptor[TextClassifier]):
    """Descriptor for a TextClassifier implementation."""


@runtime_checkable
class ImageClassifier(Protocol):
    """Protocol for image classification implementations."""

    def classify_image(self, image: list[Image], labels: Label | list[Label]) -> list[Label]:
        """Classifies a batch of images using the given label(s)."""
        ...


class ImageClassifierDescriptor(Descriptor[ImageClassifier]):
    """Descriptor for a ImageClassifier implementation."""


@runtime_checkable
class Prompter(Protocol):
    """Protocol for prompt/chat completion implementations."""

    async def prompt(self, messages: tuple[Any, ...]) -> Any:
        """Generates responses for a batch of message strings."""
        ...


class PrompterDescriptor(Descriptor[Prompter]):
    """Descriptor for a Prompter implementation."""
