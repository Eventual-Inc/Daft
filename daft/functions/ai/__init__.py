from __future__ import annotations

from typing import TYPE_CHECKING, Union

from daft import DataType, Expression, col, udf, current_session
from daft.ai import Model, get_default_provider, get_default_model

if TYPE_CHECKING:
    from daft.ai.protocols import TextEmbedderLike
    from daft.ai import Provider
    from daft.utils import ColumnInputType


ExprLike = Union[Expression, str]


def _as_expr(expr: ExprLike) -> Expression:
    return col(expr) if isinstance(expr, str) else expr


def _get_model(
    provider: Provider | None,
    model: str | None,
    fallback_provider: Provider,
    fallback_model: str,
) -> Model:
    sess = current_session()
    p = provider or sess.current_provider() or fallback_provider
    m = model or sess.current_model() or fallback_model
    return Model.from_provider(p, m)


##
# EMBED FUNCTIONS
##


def embed_text(
    text: ColumnInputType,
    *,
    provider: Provider | None = None,
    model: str | None = None,
) -> Expression:
    """Expression that embeds text using the specified model and provider.

    Args:
        text (ColumnInputType): The input text column or expression to embed.
        provider (Provider | None, optional): The provider to use for the embedding model. If None, a default provider will be used.
        model (str | None, optional): The name of the model to use for embedding. If None, a default model will be used.

    Returns:
        Expression: An expression representing the embedded text vectors.
    """
    from daft.ai._expressions import _TextEmbedderExpression, _TextEmbedderBatchedExpression
    from daft.ai.protocols import TextEmbedder, TextEmbedderBatched

    # convert all arguments to expressions
    text = _as_expr(text)

    # resolve the model and its expression implementation
    text_embedder = _get_model(
        provider,
        model,
        fallback_provider="sentence_transformers",
        fallback_model="all-MiniLM-L6-v2",
    )

    if not isinstance(text_embedder, TextEmbedder):
        text_embedder_cls = _TextEmbedderBatchedExpression
        raise ValueError(f"The model instance {text_embedder_cls} does not implement the TextEmbedder protocol.")

    expr = udf(return_dtype=text_embedder.dimensions.as_dtype(), concurrency=1)(text_embedder_cls)  # type: ignore
    expr = expr.with_init_args(text_embedder)
    return expr(text)
