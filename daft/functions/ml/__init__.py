from __future__ import annotations

from typing import TYPE_CHECKING, Union

from daft import DataType, Expression, col, udf
from daft.ml import Model, get_default_provider, get_default_model

from daft.ml._transformers import _TransformersTextEmbedder

if TYPE_CHECKING:
    from daft.ml.protocols import (
        TextClassifierLike,
        TextEmbedderLike,
        TextTransformerLike,
    )
    from daft.ml import Provider
    from daft.utils import ColumnInputType


ExprLike = Union[Expression, str]


def _as_expr(expr: ExprLike) -> Expression:
    return col(expr) if isinstance(expr, str) else expr


##
# EMBED FUNCTIONS
##


def embed_text(
    text: ColumnInputType,
    *,
    model: str | TextEmbedderLike | None = None,
    provider: Provider | None = None,
) -> Expression:
    """Expression that embeds text using the specified model and provider.

    Args:
        text (ColumnInputType): The input text column or expression to embed.
        model (str | None, optional): The name of the model to use for embedding. If None, a default model may be used.
        provider (Provider | None, optional): The provider to use for the embedding model. If None, a default provider may be used.

    Returns:
        Expression: An expression representing the embedded text vectors.
    """
    from daft.ml._expressions import _TextEmbedderExpression

    # convert all arguments to expressions
    text = _as_expr(text)

    # TODO load models from session with the runner cache
    # model_ref: TextEmbedderLike = Model._from_transformers("all-MiniLM-L6-v2") # type: ignore
    model_ref = _TransformersTextEmbedder("all-MiniLM-L6-v2")

    expr = udf(return_dtype=model_ref.embed_text_return_dtype())(_TextEmbedderExpression)
    expr = expr.with_init_args(model_ref)
    return expr(text)


##
# CLASSIFY FUNCTIONS
##


def classify_text(
    text: ColumnInputType,
    *,
    model: str | None = None,
    provider: Provider | None = None,
) -> Expression:
    """Expression that classifies text using the specified model and provider.

    Args:
        text (ColumnInputType): The input text column or expression to classify.
        model (str | None, optional): The name of the model to use for classification. If None, a default model may be used.
        provider (Provider | None, optional): The provider to use for the classification model. If None, a default provider may be used.

    Returns:
        Expression: An expression representing the classification results.
    """
    raise NotImplementedError()


##
# TRANSFORM FUNCTIONS
##


def transform_text(
    text: ColumnInputType,
    *,
    model: str | None = None,
    provider: Provider | None = None,
) -> Expression:
    """Expression that transforms text using the specified model and provider.

    Args:
        text (ColumnInputType): The input text column or expression to transform.
        model (str | None, optional): The name of the model to use for transformation. If None, a default model may be used.
        provider (Provider | None, optional): The provider to use for the transformation model. If None, a default provider may be used.

    Returns:
        Expression: An expression representing the transformed text.
    """
    raise NotImplementedError()
