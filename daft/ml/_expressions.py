from __future__ import annotations

from typing import TYPE_CHECKING

from daft.dependencies import np

if TYPE_CHECKING:
    from daft import Series
    from daft.ml.protocols import TextEmbedder


class _TextEmbedderExpression:
    """Function expression implementation for a TextEmbedder protocol."""

    _model: TextEmbedder

    def __init__(self, model: TextEmbedder):
        self._model = model
        self._model.__enter__() # initialize

    def __call__(self, text_series: Series) -> list[np.ndarray]:
        return [ self._model.embed_text(text) for text in text_series ]


class _TextEmbedderExpression:
    """Function expression implementation for a TextEmbedder protocol."""

    _model: TextEmbedder

    def __init__(self, model: TextEmbedder):
        self._model = model
        self._model.__enter__() # initialize

    def __call__(self, text_series: Series) -> np.ndarray:
        array = text_series.to_arrow().to_numpy(zero_copy_only=False)
        if (len(array) == 0):
            return np.empty(np.int32)
        return self._model.embed_text_batched(array)
