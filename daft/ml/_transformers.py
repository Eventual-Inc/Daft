from __future__ import annotations

from typing import TYPE_CHECKING

import torch
from sentence_transformers import SentenceTransformer

from daft import DataType
from daft.ml.model import Model
from daft.ml.protocols import TextEmbedder

if TYPE_CHECKING:
    from daft.dependencies import np
    from daft.ml.typing import Embedding


class _Transformers(Model):
    """Transformers-based implementation of the Model protocol."""

    _model: str

    def __init__(self, model: str, **properties: str):
        self._model = model


class _TransformersTextEmbedder(TextEmbedder):

    _model: str
    _device: str
    _sentence_transformer: SentenceTransformer | None = None

    def __init__(self, model: str):
        self._model = model

    def __enter__(self) -> None:
        """Initializes a single instance of the SentenceTransformer."""
        if self._sentence_transformer is not None:
            return
        self._device = 'cuda' if torch.cuda.is_available() else 'cpu'
        self._sentence_transformer = SentenceTransformer(self._model, device=self._device)

    def __exit__(self) -> None:
        """Removes the sole reference to the inner sentence transformer."""
        del self._sentence_transformer

    def embed_text(self, text: str) -> Embedding:
        """Embeds a single string of text into an embedding vector."""
        if self._sentence_transformer is None:
            raise RuntimeError("Cannot use model outside a context manager.")
        return self._sentence_transformer.encode(text) # type: ignore

    def embed_text_batched(self, text: np.ndarray) -> Embedding:
        """Embeds a single string of text into an embedding vector."""
        if self._sentence_transformer is None:
            raise RuntimeError("Cannot use model outside a context manager.")
        return self._sentence_transformer.encode(text) # type: ignore

    def embed_text_return_dtype(self) -> DataType:
        # hardcoding all-MiniLM-L6-v2
        return DataType.embedding(dtype=DataType.int32(), size=384)
