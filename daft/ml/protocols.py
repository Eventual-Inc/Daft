from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from daft.ml.model import Model

if TYPE_CHECKING:
    from daft.datatype import DataType
    from daft.dependencies import np
    from daft.ml.typing import Embedding

##
# EMBEDDER PROTOCOLS
##


@runtime_checkable
class TextEmbedder(Model, Protocol):
    """Protocol for a text embedding model that processes single strings."""

    def embed_text(self, text: str) -> Embedding:
        """Embeds a single string of text into an embedding vector."""
        ...

    def embed_text_batched(self, text: np.ndarray) -> Embedding:
        """Embeds a single string of text into an embedding vector."""
        ...

    def embed_text_return_dtype(self) -> DataType:
        """Returns the `embed_text` return type for this implementation."""
        ...


TextEmbedderLike = TextEmbedder


##
# CLASSIFIER PROTOCOLS
##


@runtime_checkable
class TextClassifier(Protocol):
    """Models that can classify text content."""

    def classify_text(self, text: str, labels: list[str] | None = None) -> dict[str, float]:
        """Classify text content."""
        ...


TextClassifierLike = TextClassifier


##
# TRANSFORMER PROTOCOLS
##


@runtime_checkable
class TextTransformer(Protocol):
    """Models that can transform text content for feature extraction."""

    def transform_text(self, text: str) -> str:
        """Transform text (summarize, translate, restyle, etc.)."""
        ...


TextTransformerLike = TextTransformer
