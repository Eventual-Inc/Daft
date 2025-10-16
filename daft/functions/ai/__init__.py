"""AI Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Union

from daft import (
    DataType,
    Expression,
    Series,
    col,
    udf,
    current_session,
    current_provider,
)
from daft.ai.provider import load_provider
from daft.ai.provider import Provider

if TYPE_CHECKING:
    from pydantic import BaseModel

    from daft.ai.protocols import TextEmbedderDescriptor
    from daft.utils import ColumnInputType
    from daft.ai.typing import Label

__all__ = [
    "classify_text",
    "embed_image",
    "embed_text",
    "prompt",
]


def _resolve_provider(provider: str | Provider | None, default: str) -> Provider:
    """Attempts to resolve a provider based upon the active session and environment variables.

    Note:
        This simply checks if the user has configured anything, then uses the provided default.
        We can choose to improve (or not) the smart's of this method like looking for the OPENAI_API_KEY
        or seeing which dependencies are available. For now, this is explicit in how the provider is resolved.
    """
    if provider is not None and isinstance(provider, Provider):
        # 0. Given a provider..
        return provider
    if provider is not None and (curr_sess := current_session()) and (curr_sess.has_provider(provider)):
        # 1. Load the provider from the active session.
        return curr_sess.get_provider(provider)
    elif provider is not None:
        # 2. Load a known provider.
        return load_provider(provider)
    elif curr_provider := current_provider():
        # 3. Use the session's current provider, if any.
        return curr_provider
    else:
        # 4. Load the default provider for this API.
        return load_provider(default)


##
# EMBED FUNCTIONS
##


def embed_text(
    text: Expression,
    *,
    provider: str | Provider | None = None,
    model: str | None = None,
    **options: str,
) -> Expression:
    """Returns an expression that embeds text using the specified embedding model and provider.

    Args:
        text (String Expression):
            The input text column expression.
        provider (str | Provider | None):
            The provider to use for the embedding model. If None, the default provider is used.
        model (str | None):
            The embedding model to use. Can be a model instance or a model name. If None, the default model is used.
        **options: Any additional options to pass for the model.

    Note:
        Make sure the required provider packages are installed (e.g. vllm, transformers, openai).

    Returns:
        Expression (Embedding Expression): An expression representing the embedded text vectors.
    """
    from daft.ai._expressions import _TextEmbedderExpression
    from daft.ai.protocols import TextEmbedder

    # load a TextEmbedderDescriptor from the resolved provider
    text_embedder = _resolve_provider(provider, "sentence_transformers").get_text_embedder(model, **options)

    # implemented as a class-based udf for now
    udf_options = text_embedder.get_udf_options()
    expr_callable = udf(
        return_dtype=text_embedder.get_dimensions().as_dtype(),
        concurrency=udf_options.concurrency,
        num_gpus=udf_options.num_gpus,
    )

    expr = expr_callable(_TextEmbedderExpression)
    expr = expr.with_init_args(text_embedder)
    return expr(text)


def embed_image(
    image: Expression,
    *,
    provider: str | Provider | None = None,
    model: str | None = None,
    **options: str,
) -> Expression:
    """Returns an expression that embeds images using the specified image model and provider.

    Args:
        image (Image Expression): The input image column expression.
        provider (str | Provider | None): The provider to use for the image model. If None, the default provider is used.
        model (str | None): The image model to use. Can be a model instance or a model name. If None, the default model is used.
        **options: Any additional options to pass for the model.

    Note:
        Make sure the required provider packages are installed (e.g. vllm, transformers, openai).

    Returns:
        Expression (Embedding Expression): An expression representing the embedded image vectors.
    """
    from daft.ai._expressions import _ImageEmbedderExpression
    from daft.ai.protocols import ImageEmbedder

    image_embedder = _resolve_provider(provider, "transformers").get_image_embedder(model, **options)

    # implemented as a class-based udf for now
    udf_options = image_embedder.get_udf_options()
    expr_udf = udf(
        return_dtype=image_embedder.get_dimensions().as_dtype(),
        concurrency=udf_options.concurrency,
        num_gpus=udf_options.num_gpus,
    )

    expr = expr_udf(_ImageEmbedderExpression)
    expr = expr.with_init_args(image_embedder)
    return expr(image)


##
# CLASSIFY FUNCTIONS
##


def classify_text(
    text: Expression,
    labels: Label | list[Label],
    *,
    provider: str | Provider | None = None,
    model: str | None = None,
    **options: str,
) -> Expression:
    """Returns an expression that classifies text using the specified model and provider.

    Args:
        text (String Expression): The input text column expression.
        labels (str | list[str]): Label(s) for classification.
        provider (str | Provider | None): The provider to use for the embedding model. If None, the default provider is used.
        model (str | None): The embedding model to use. Can be a model instance or a model name. If None, the default model is used.
        **options: Any additional options to pass for the model.

    Note:
        Make sure the required provider packages are installed (e.g. vllm, transformers, openai).

    Returns:
        Expression (String Expression): An expression representing the most-probable label string.
    """
    from daft.ai._expressions import _TextClassificationExpression
    from daft.ai.protocols import TextClassifier

    text_classifier = _resolve_provider(provider, "transformers").get_text_classifier(model, **options)

    # TODO(rchowell): classification with structured outputs will be more interesting
    label_list = [labels] if isinstance(labels, str) else labels

    # implemented as a class-based udf for now
    udf_options = text_classifier.get_udf_options()
    expr_callable = udf(
        return_dtype=DataType.string(),
        concurrency=udf_options.concurrency,
        num_gpus=udf_options.num_gpus,
    )

    expr = expr_callable(_TextClassificationExpression)
    expr = expr.with_init_args(text_classifier, label_list)
    return expr(text)


##
# PROMPT FUNCTIONS
##


def prompt(
    messages: Expression,
    return_format: BaseModel | None = None,
    *,
    system_message: str | None = None,
    provider: str | Provider | None = None,
    model: str | None = None,
    **options: str,
) -> Expression:
    from daft.udf import cls as daft_cls, method
    from daft.ai._expressions import _PrompterExpression

    # Add return_format to options for the provider
    if return_format is not None:
        options = {**options, "return_format": return_format}
    if system_message is not None:
        options = {**options, "system_message": system_message}

    # Load a PrompterDescriptor from the resolved provider
    prompter_descriptor = _resolve_provider(provider, "openai").get_prompter(model, **options)

    # Determine return dtype
    if return_format is not None:
        try:
            return_dtype = DataType.infer_from_type(return_format)
        except Exception:
            return_dtype = DataType.string()
    else:
        return_dtype = DataType.string()

    # Get UDF options from the descriptor
    udf_options = prompter_descriptor.get_udf_options()

    # Decorate the __call__ method with @daft.method to specify return_dtype
    _PrompterExpression.__call__ = method(method=_PrompterExpression.__call__, return_dtype=return_dtype)  # type: ignore[method-assign]

    # Wrap the class with @daft.cls
    wrapped_cls = daft_cls(
        _PrompterExpression,
        gpus=udf_options.num_gpus or 0,
        max_concurrency=udf_options.concurrency,
    )

    # Instantiate the wrapped class with the prompter descriptor
    instance = wrapped_cls(prompter_descriptor)

    # Call the instance (which calls __call__ method) with the messages expression
    return instance(messages)
