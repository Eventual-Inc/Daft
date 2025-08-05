from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft import Series
    from daft.ml.protocols import TextEmbedder, TextEmbedderBatched
    from daft.ml.typing import Embedding


class _TextEmbedderExpression:
    """Function expression implementation for a TextEmbedder protocol."""

    _model: TextEmbedder

    def __init__(self, model: TextEmbedder):
        self._model = model
        self._model.__enter__()  # initialize

    def __call__(self, text_series: Series) -> list[Embedding]:
        return [self._model.embed_text(text) for text in text_series]


class _TextEmbedderBatchedExpression:
    """Function expression implementation for a TextEmbedder protocol."""

    _model: TextEmbedderBatched

    def __init__(self, model: TextEmbedderBatched):
        self._model = model
        self._model.__enter__()  # initialize

    def __call__(self, text_series: Series) -> list[Embedding]:
        text = text_series.to_pylist()
        return self._model.embed_text(text) if text else []
