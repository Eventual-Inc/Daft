from __future__ import annotations

from typing import TYPE_CHECKING

import torch
from sentence_transformers import SentenceTransformer

from daft import DataType
from daft.ml.protocols import TextEmbedderBatched
from daft.ml.typing import EmbeddingDimensions

if TYPE_CHECKING:
    from daft.ml.typing import Embedding


class _SentenceTransformers(TextEmbedderBatched):
    _model: str
    _device: str
    _sentence_transformer: SentenceTransformer | None = None

    def __init__(self, model: str):
        self._model = model

    def __enter__(self) -> None:
        """Initializes a single instance of the SentenceTransformer."""
        if self._sentence_transformer is not None:
            return
        self._device = "cuda" if torch.cuda.is_available() else "cpu"
        self._sentence_transformer = SentenceTransformer(self._model, trust_remote_code=True)
        self._sentence_transformer.to(self._device)  # import to move _after_ loading

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Removes the sole reference to the inner sentence transformer."""
        if self._sentence_transformer is not None:
            del self._sentence_transformer
            self._sentence_transformer = None
            if self._device == "cuda" and torch.cuda.is_available():
                torch.cuda.empty_cache()

    @property
    def dimensions(self) -> EmbeddingDimensions:
        # hardcoding all-MiniLM-L6-v2
        return EmbeddingDimensions(384, dtype=DataType.float32())

    def embed_text(self, text: list[str]) -> list[Embedding]:
        """Embeds a single string of text into an embedding vector."""
        if self._sentence_transformer is None:
            raise RuntimeError("Cannot use model outside a context manager.")
        batch = self._sentence_transformer.encode(text, convert_to_numpy=True)  # type: ignore
        return list(batch)
