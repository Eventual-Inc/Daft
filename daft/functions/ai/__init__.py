from __future__ import annotations

from typing import TYPE_CHECKING, Union

from daft import DataType, Expression, col, udf, current_session
from daft.ai.provider import load_sentence_transformers

if TYPE_CHECKING:
    from daft.ai.protocols import TextEmbedderDescriptor
    from daft.ai.provider import Provider
    from daft.utils import ColumnInputType


def _as_expr(column: ColumnInputType) -> Expression:
    return col(column) if isinstance(column, str) else column


##
# EMBED FUNCTIONS
##


def embed_text(
    text: ColumnInputType,
    *,
    provider: str | None = None,
    model: str | None = None,
    **options: str,
) -> Expression:
    """Returns an expression that embeds text using the specified embedding model and provider.

    Args:
        text (ColumnInputType): The input text column, expression, or string to embed.
        model (str | None, optional): The embedding model to use. Can be a model instance or a model name. If None, the default model is used.
        provider (Provider | None, optional): The provider to use for the embedding model. If None, the default provider is used.
        **properties: Any additional properties to pass to the model constructor.

    Note:
        Make sure the required provider packages are installed (e.g. vllm, transformers, openai).

    Returns:
        Expression: An expression representing the embedded text vectors.
    """
    from daft.ai._expressions import _TextEmbedderExpression
    from daft.ai.protocols import TextEmbedder

    # convert all arguments to expressions
    text = _as_expr(text)

    text_embedder = load_sentence_transformers({}).get_text_embedder(model)

    # implemented as class-based UDF for now
    expr = udf(return_dtype=text_embedder.get_dimensions().as_dtype(), concurrency=1)(_TextEmbedderExpression)
    expr = expr.with_init_args(text_embedder)
    return expr(text)
