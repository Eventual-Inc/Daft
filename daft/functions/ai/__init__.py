from __future__ import annotations

from typing import TYPE_CHECKING, Union

from daft import DataType, Expression, col, udf, current_session
from daft.ai.provider import load_provider
from daft.session import current_provider

if TYPE_CHECKING:
    from daft.ai.protocols import TextEmbedderDescriptor
    from daft.ai.provider import Provider
    from daft.utils import ColumnInputType


def _as_expr(column: ColumnInputType) -> Expression:
    return col(column) if isinstance(column, str) else column


def _get_provider(provider: str | None, default: str) -> Provider:
    """Attempts to resolve a provider based upon the active session and environment variables.

    Note:
        This simply checks if the user has configured anything, then uses the provided default.
        We can choose to improve (or not) the smart's of this method like looking for the OPENAI_API_KEY
        or seeing which dependencies are available. For now, this is explicit in how the provider is resolved.

        There are also no options being passed to the provider loading. This will need to be modified
        when necessary.
    """
    return load_provider(provider or current_provider() or default)


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
        model (str | None): The embedding model to use. Can be a model instance or a model name. If None, the default model is used.
        provider (str | None): The provider to use for the embedding model. If None, the default provider is used.
        **options: Any additional options to pass for the model.

    Note:
        Make sure the required provider packages are installed (e.g. vllm, transformers, openai).

    Returns:
        Expression: An expression representing the embedded text vectors.
    """
    from daft.ai._expressions import _TextEmbedderExpression
    from daft.ai.protocols import TextEmbedder

    # convert all arguments to expressions
    text = _as_expr(text)

    text_embedder = _get_provider(provider, "sentence_transformers").get_text_embedder(model, **options)

    # implemented as class-based UDF for now
    expr = udf(return_dtype=text_embedder.get_dimensions().as_dtype(), concurrency=1)(_TextEmbedderExpression)
    expr = expr.with_init_args(text_embedder)
    return expr(text)
