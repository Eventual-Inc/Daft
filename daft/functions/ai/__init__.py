"""AI Functions."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, Literal, overload


if sys.version_info < (3, 11):
    from typing_extensions import Unpack
else:
    from typing import Unpack

from daft.ai.provider import Provider, ProviderType, load_provider
from daft.functions.ai._colab_compat import IS_COLAB, clean_pydantic_model
from daft.datatype import DataType
from daft.expressions import Expression
from daft.session import current_provider, current_session
from daft.udf import cls as daft_cls, method

if TYPE_CHECKING:
    from pydantic import BaseModel
    from daft.ai.typing import (
        ClassifyImageOptions,
        ClassifyTextOptions,
        EmbedImageOptions,
        EmbedTextOptions,
        Label,
        PromptOptions,
    )
    from daft.ai.openai.provider import OpenAIProvider
    from daft.ai.openai.protocols.prompter import OpenAIPromptOptions

__all__ = [
    "classify_image",
    "classify_text",
    "embed_image",
    "embed_text",
    "prompt",
]


def _resolve_provider(provider: str | Provider | None, default: ProviderType) -> Provider:
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
    dimensions: int | None = None,
    **options: Unpack[EmbedTextOptions],
) -> Expression:
    """Returns an expression that embeds text using the specified embedding model and provider.

    Args:
        text (String Expression):
            The input text column expression.
        provider (str | Provider | None):
            The provider to use for the embedding model. If None, the default provider is used.
        model (str | None):
            The embedding model to use. Can be a model instance or a model name. If None, the default model is used.
        dimensions (int | None):
            Number of dimensions the output embeddings should have, if the provider and model support specifying. If None, will use the default for the model.
        **options: Any additional options to pass for the model.

    Note:
        Make sure the required provider packages are installed (e.g. vllm, transformers, openai).

    Returns:
        Expression (Embedding Expression): An expression representing the embedded text vectors.

    Examples:
        >>> import daft
        >>> from daft.functions import embed_text
        >>> df = daft.read_huggingface("togethercomputer/RedPajama-Data-1T")
        >>> # Embed Text with Defaults
        >>> df = df.with_column(
        ...     "embeddings",
        ...     embed_text(
        ...         daft.col("text"),
        ...         provider="transformers",
        ...         model="sentence-transformers/all-MiniLM-L6-v2",
        ...     ),
        ... )
        >>> df.limit(3).show()
        ╭────────────────────────────────┬────────────────────────────────┬───────────────────┬──────────────────────────╮
        │ text                           ┆ meta                           ┆ red_pajama_subset ┆ embeddings               │
        │ ---                            ┆ ---                            ┆ ---               ┆ ---                      │
        │ String                         ┆ String                         ┆ String            ┆ Embedding[Float32; 384]  │
        ╞════════════════════════════════╪════════════════════════════════╪═══════════════════╪══════════════════════════╡
        │ Григорианският календар (поня… ┆ {'title': 'Григориански кален… ┆ wikipedia         ┆ ▃▆█▆▆▆█▇▆▅▃▆▆▅▅▆▅▅▂▂▇▇▄▁ │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ GNU General Public License (н… ┆ {'title': 'GNU General Public… ┆ wikipedia         ┆ ▆▁▇█▄▅▄▅▄▄▁▆▃▅▂▃▆▃▄▃█▆▇▅ │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ Лицензът за свободна документ… ┆ {'title': 'Лиценз за свободна… ┆ wikipedia         ┆ ▄▆██▇▇▇█▇▆▂▇▄▁▅▃▇▇▃▃▆▆▅▂ │
        ╰────────────────────────────────┴────────────────────────────────┴───────────────────┴──────────────────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    from daft.ai._expressions import _TextEmbedderExpression

    # load a TextEmbedderDescriptor from the resolved provider
    text_embedder = _resolve_provider(provider, "transformers").get_text_embedder(model, dimensions, **options)

    udf_options = text_embedder.get_udf_options()

    # Choose synchronous or asynchronous call implementation based on the embedder
    is_async = text_embedder.is_async()
    call_impl = _TextEmbedderExpression._call_async if is_async else _TextEmbedderExpression._call_sync

    # Decorate the selected call method with @daft.method to specify return_dtype
    _TextEmbedderExpression.__call__ = method.batch(  # type: ignore[method-assign]
        method=call_impl,
        return_dtype=text_embedder.get_dimensions().as_dtype(),
        batch_size=udf_options.batch_size,
    )
    wrapped_cls = daft_cls(
        _TextEmbedderExpression,
        max_concurrency=udf_options.concurrency,
        gpus=udf_options.num_gpus or 0,
        max_retries=udf_options.max_retries,
        on_error=udf_options.on_error,
    )

    expr = wrapped_cls(text_embedder)
    return expr(text)


def embed_image(
    image: Expression,
    *,
    provider: str | Provider | None = None,
    model: str | None = None,
    **options: Unpack[EmbedImageOptions],
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

    Examples:
        >>> import daft
        >>> from daft.functions import embed_image, decode_image
        >>> df = (
        ...     # Discover a few images from HuggingFace
        ...     daft.from_glob_path("hf://datasets/datasets-examples/doc-image-3/images")
        ...     # Read the 4 PNG, JPEG, TIFF, WEBP Images
        ...     .with_column("image_bytes", daft.col("path").download())
        ...     # Decode the image bytes into a daft Image DataType
        ...     .with_column("image_type", decode_image(daft.col("image_bytes")))
        ...     # Convert Image to RGB and resize the image to 288x288
        ...     .with_column("image_resized", daft.col("image_type").convert_image("RGB").resize(288, 288))
        ...     # Embed the image
        ...     .with_column(
        ...         "image_embeddings",
        ...         embed_image(
        ...             daft.col("image_resized"), provider="transformers", model="apple/aimv2-large-patch14-224-lit"
        ...         ),
        ...     )
        ... )
        >>> df.show()
        ╭────────────────────────────────┬─────────┬───────────────┬──────────────┬───────────────────────┬──────────────────────────╮
        │ path                           ┆ size    ┆ image_bytes   ┆ image_type   ┆ image_resized         ┆ image_embeddings         │
        │ ---                            ┆ ---     ┆ ---           ┆ ---          ┆ ---                   ┆ ---                      │
        │ String                         ┆ Int64   ┆ Binary        ┆ Image[MIXED] ┆ Image[RGB; 288 x 288] ┆ Embedding[Float32; 768]  │
        ╞════════════════════════════════╪═════════╪═══════════════╪══════════════╪═══════════════════════╪══════════════════════════╡
        │ hf://datasets/datasets-exampl… ┆ 113469  ┆ ...           ┆ <Image>      ┆ <FixedShapeImage>     ┆ ▃▅▅▆▆▂▅▆▅▇█▂▂▄▅▂▆▃▃▅▁▇▃▅ │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ hf://datasets/datasets-exampl… ┆ 206898  ┆ ...           ┆ <Image>      ┆ <FixedShapeImage>     ┆ ▃▃▄▆▄▅▃▄▅▅▅▃▂▇▁▁▁▂▃▅▄█▃▅ │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ hf://datasets/datasets-exampl… ┆ 1871034 ┆ ...           ┆ <Image>      ┆ <FixedShapeImage>     ┆ ▂▃▃▃▄▄▃▆▆▄▅▂▁▃▁▄▃▅▄▄▂█▆▆ │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ hf://datasets/datasets-exampl… ┆ 22022   ┆ ...           ┆ <Image>      ┆ <FixedShapeImage>     ┆ ▄▂▂▅▆▆▅▇▆▄▅▆▃▅▅▁▃▄▄▄▃█▃▆ │
        ╰────────────────────────────────┴─────────┴───────────────┴──────────────┴───────────────────────┴──────────────────────────╯
        <BLANKLINE>
        (Showing first 4 of 4 rows)
    """
    from daft.ai._expressions import _ImageEmbedderExpression

    image_embedder = _resolve_provider(provider, "transformers").get_image_embedder(model, **options)

    udf_options = image_embedder.get_udf_options()

    # Decorate the __call__ method with @daft.method to specify return_dtype
    _ImageEmbedderExpression.__call__ = method.batch(  # type: ignore[method-assign] # type: ignore[method-assign] # type: ignore[method-assign]
        method=_ImageEmbedderExpression.__call__,
        return_dtype=image_embedder.get_dimensions().as_dtype(),
    )

    wrapped_cls = daft_cls(
        _ImageEmbedderExpression,
        max_concurrency=udf_options.concurrency,
        gpus=udf_options.num_gpus or 0,
        max_retries=udf_options.max_retries,
        on_error=udf_options.on_error,
    )

    expr = wrapped_cls(image_embedder)
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
    **options: Unpack[ClassifyTextOptions],
) -> Expression:
    """Returns an expression that classifies text using the specified model and provider.

    Args:
        text (String Expression):
            The input text column expression.
        labels (str | list[str]):
            Label(s) for classification.
        provider (str | Provider | None):
            The provider to use for the embedding model.
            By default this will use 'transformers' provider
        model (str | None):
            The classifier model to use. Can be a model instance or a model name.
            By default this will use `zero-shot-classification` model
        **options:
            Any additional options to pass for the model.

    Note:
        Make sure the required provider packages are installed (e.g. vllm, transformers, openai).

    Returns:
        Expression (String Expression): An expression representing the most-probable label string.

    Examples:
        >>> import daft
        >>> from daft.functions import classify_text
        >>> df = daft.from_pydict({"text": ["Daft is wicked fast!"]})
        >>> df = df.with_column(
        ...     "label",
        ...     classify_text(
        ...         daft.col("text"),
        ...         labels=["Positive", "Negative"],
        ...         provider="transformers",
        ...         model="tabularisai/multilingual-sentiment-analysis",
        ...     ),
        ... )
        >>> df.show()
        ╭─────────────────────┬───────────╮
        │ text                ┆ label     │
        │ ---                 ┆ ---       │
        │ String              ┆ String    │
        ╞═════════════════════╪═══════════╡
        │ Daft is wicked fast!┆ Positive  │
        ╰─────────────────────┴───────────╯
        <BLANKLINE>
        (Showing first 1 of 1 rows)
    """
    from daft.ai._expressions import _TextClassificationExpression

    text_classifier = _resolve_provider(provider, "transformers").get_text_classifier(model, **options)

    # TODO(rchowell): classification with structured outputs will be more interesting
    label_list = [labels] if isinstance(labels, str) else labels

    udf_options = text_classifier.get_udf_options()
    # Decorate the __call__ method with @daft.method to specify return_dtype
    _TextClassificationExpression.__call__ = method.batch(  # type: ignore[method-assign]
        method=_TextClassificationExpression.__call__, return_dtype=DataType.string()
    )
    wrapped_cls = daft_cls(
        _TextClassificationExpression,
        max_concurrency=udf_options.concurrency,
        gpus=udf_options.num_gpus or 0,
        max_retries=udf_options.max_retries,
        on_error=udf_options.on_error,
    )

    expr = wrapped_cls(text_classifier, label_list)
    return expr(text)


def classify_image(
    image: Expression,
    labels: Label | list[Label],
    *,
    provider: str | Provider | None = None,
    model: str | None = None,
    **options: Unpack[ClassifyImageOptions],
) -> Expression:
    """Returns an expression that classifies images using the specified model and provider.

    Args:
        image (Image Expression):
            The input image column expression.
        labels (str | list[str]):
            Label(s) for classification.
        provider (str | Provider | None):
            The provider to use for the embedding model.
            By default this will use 'transformers' provider
        model (str | None):
            The classifier model to use. Can be a model instance or a model name.
            By default this will use `zero-shot-classification` model
        **options:
            Any additional options to pass for the model.

    Note:
        Make sure the required provider packages are installed (e.g. vllm, transformers, openai).

    Returns:
        Expression (String Expression): An expression representing the most-probable label string.

    Examples:
        >>> import daft
        >>> from daft.functions import classify_image, decode_image
        >>> df = (
        ...     # Discover a few images from HuggingFace
        ...     daft.from_glob_path("hf://datasets/datasets-examples/doc-image-3/images")
        ...     # Read the 4 PNG, JPEG, TIFF, WEBP Images
        ...     .with_column("image_bytes", daft.col("path").download())
        ...     # Decode the image bytes into a daft Image DataType
        ...     .with_column("image_type", decode_image(daft.col("image_bytes")))
        ...     # Convert Image to RGB and resize the image to 288x288
        ...     .with_column("image_resized", daft.col("image_type").convert_image("RGB").resize(288, 288))
        ...     # Classify the image
        ...     .with_column(
        ...         "image_label",
        ...         classify_image(
        ...             daft.col("image_resized"),
        ...             labels=["bulbasaur", "catapie", "voltorb", "electrode"],
        ...             provider="transformers",
        ...             model="openai/clip-vit-base-patch32",
        ...         ),
        ...     )
        ... )
        >>> df.show()
        ╭────────────────────────────────┬─────────┬────────────────┬──────────────┬───────────────────────┬───────────────╮
        │ path                           ┆ size    ┆ image_bytes    ┆ image_type   ┆ image_resized         ┆ image_labels  │
        │ ---                            ┆ ---     ┆ ---            ┆ ---          ┆ ---                   ┆ ---           │
        │ String                         ┆ Int64   ┆ Binary         ┆ Image[MIXED] ┆ Image[RGB; 288 x 288] ┆ String        │
        ╞════════════════════════════════╪═════════╪════════════════╪══════════════╪═══════════════════════╪═══════════════╡
        │ hf://datasets/datasets-exampl… ┆ 113469  ┆ ...            ┆ <Image>      ┆ <FixedShapeImage>     ┆ bulbasaur     │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ hf://datasets/datasets-exampl… ┆ 206898  ┆ ...            ┆ <Image>      ┆ <FixedShapeImage>     ┆ catapie       │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ hf://datasets/datasets-exampl… ┆ 1871034 ┆ ...            ┆ <Image>      ┆ <FixedShapeImage>     ┆ voltorb       │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ hf://datasets/datasets-exampl… ┆ 22022   ┆ ...            ┆ <Image>      ┆ <FixedShapeImage>     ┆ electrode     │
        ╰────────────────────────────────┴─────────┴────────────────┴──────────────┴───────────────────────┴───────────────╯
        <BLANKLINE>
        (Showing first 4 of 4 rows)
    """
    from daft.ai._expressions import _ImageClassificationExpression

    image_classifier = _resolve_provider(provider, "transformers").get_image_classifier(model, **options)

    # TODO: classification with structured outputs will be more interesting
    label_list = [labels] if isinstance(labels, str) else labels
    # Decorate the __call__ method with @daft.method to specify return_dtype
    _ImageClassificationExpression.__call__ = method.batch(  # type: ignore[method-assign]
        method=_ImageClassificationExpression.__call__,
        return_dtype=DataType.string(),
    )
    # implemented as a class-based udf for now
    udf_options = image_classifier.get_udf_options()
    wrapped_cls = daft_cls(
        _ImageClassificationExpression,
        max_concurrency=udf_options.concurrency,
        gpus=udf_options.num_gpus or 0,
        max_retries=udf_options.max_retries,
        on_error=udf_options.on_error,
    )
    instance = wrapped_cls(image_classifier, label_list)
    return instance(image)


##
# PROMPT FUNCTIONS
##


@overload
def prompt(
    messages: list[Expression] | Expression,
    return_format: BaseModel | None = None,
    *,
    system_message: str | None = None,
    provider: Literal["openai"] | OpenAIProvider,
    model: str | None = None,
    **options: Unpack[OpenAIPromptOptions],
) -> Expression: ...


@overload
def prompt(
    messages: list[Expression] | Expression,
    return_format: BaseModel | None = None,
    *,
    system_message: str | None = None,
    provider: str | None,
    model: str | None = None,
    **options: Unpack[PromptOptions],
) -> Expression: ...


def prompt(
    messages: list[Expression] | Expression,
    return_format: BaseModel | None = None,
    *,
    system_message: str | None = None,
    provider: str | Provider | None = None,
    model: str | None = None,
    **options: Any,
) -> Expression:
    """Returns an expression that prompts a large language model using the specified model and provider.

    Args:
        messages (list[Expression] | Expression): The list of messages to prompt the model with. Each expression can be either:
            - Plain text strings (always treated as input_text)
            - Image data (numpy arrays, bytes, or File objects - detected by MIME type)
            - Files (PDF, TXT, HTML, audio, video, etc.) as bytes or File objects (detected by MIME type)
        return_format (BaseModel | None): The return format for the prompt. Use a Pydantic model for structured outputs.
        system_message (str | None): The system message for the prompt.
        provider (str | Provider | None): The provider to use for the prompt (default: "openai").
        model (str | None): The model to use for the prompt.
        **options: Any additional options to pass for the prompt.

    Returns:
        Expression (String Expression): An expression representing the prompt result.

    Examples:
        Basic Usage:
        >>> import daft
        >>> from daft.ai.openai.provider import OpenAIProvider
        >>> from daft.functions.ai import prompt
        >>> # Create a dataframe with the quotes
        >>> df = daft.from_pydict(
        ...     {
        ...         "quote": [
        ...             "I am going to be the king of the pirates!",
        ...             "I'm going to be the next Hokage!",
        ...         ],
        ...     }
        ... )
        >>> # Use the prompt function to classify the quotes
        >>> df = df.with_column(
        ...     "response",
        ...     prompt(
        ...         daft.col("quote"),
        ...         system_message="Classify the anime from the quote and return the show, character name, and explanation.",
        ...         provider="openai",  # Make sure OPENAI_API_KEY is set
        ...         model="gpt-5-nano",
        ...     ),
        ... )
        >>> df.show(format="fancy", max_width=120)
        ╭───────────────────────────────────────────┬─────────────────────────────────────────────────────────╮
        │ quote                                     ┆ response                                                │
        ╞═══════════════════════════════════════════╪═════════════════════════════════════════════════════════╡
        │ I am going to be the king of the pirates! ┆ **Anime Name:** *One Piece*                             │
        │                                           ┆ **Character:** Monkey D. Luffy                          │
        │                                           ┆ **Quote:** "I am going to be the king of the pirates!"… │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ I'm going to be the next Hokage!          ┆ **Name:** Naruto                                        │
        │                                           ┆ **Character:** Naruto Uzumaki                           │
        │                                           ┆ **Quote:** *"I'm going to be the next Hokage!"*         │
        │                                           ┆                                                         │
        │                                           ┆ This quote refl…                                        │
        ╰───────────────────────────────────────────┴─────────────────────────────────────────────────────────╯

        Structured Outputs with Custom OpenAI Provider:
        >>> import os
        >>> from dotenv import load_dotenv
        >>> import daft
        >>> from daft.ai.openai.provider import OpenAIProvider
        >>> from daft.functions.ai import prompt
        >>> from daft.functions import unnest
        >>> from daft.session import Session
        >>> from pydantic import BaseModel, Field
        >>> # Load environment variables
        >>> load_dotenv()
        >>> class Anime(BaseModel):
        >>>     show: str = Field(description="The name of the anime show")
        >>>     character: str = Field(description="The name of the character who says the quote")
        >>>     explanation: str = Field(description="Why the character says the quote")
        ...
        >>> # Create an OpenRouter provider
        >>> openrouter_provider = OpenAIProvider(
        ...     name="OpenRouter", base_url="https://openrouter.ai/api/v1", api_key=os.environ.get("OPENROUTER_API_KEY")
        ... )
        >>> # Create a session and attach the provider
        >>> sess = Session()
        >>> sess.attach_provider(openrouter_provider)
        >>> sess.set_provider("OpenRouter")
        >>> # Create a dataframe with the quotes
        >>> df = daft.from_pydict(
        ...     {
        ...         "quote": [
        ...             "I am going to be the king of the pirates!",
        ...             "I'm going to be the next Hokage!",
        ...         ],
        ...     }
        ... )
        >>> # Use the prompt function to classify the quotes
        >>> df = df.with_column(
        ...     "nemotron-response",
        ...     prompt(
        ...         daft.col("quote"),
        ...         system_message="Classify the anime from the quote and return the show, character name, and explanation.",
        ...         return_format=Anime,
        ...         provider=sess.get_provider("OpenRouter"),
        ...         model="nvidia/nemotron-nano-9b-v2:free",
        ...     ),
        ... ).select("quote", unnest(daft.col("nemotron-response")))
        >>> df.show(format="fancy", max_width=120)
        ╭───────────────────────────────────────────┬───────────┬─────────────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
        │ quote                                     ┆ show      ┆ character       ┆ explanation                                                                                                            │
        ╞═══════════════════════════════════════════╪═══════════╪═════════════════╪════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╡
        │ I am going to be the king of the pirates! ┆ One Piece ┆ Monkey D. Luffy ┆ Luffy famously states his dream of becoming the Pirate King throughout the series.                                     │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ I'm going to be the next Hokage!          ┆ Naruto    ┆ Naruto Uzumaki  ┆ The phrase 'I'm going to be the next Hokage!' is a recurring aspiration in the *Naruto* series, particularly voiced b… │
        ╰───────────────────────────────────────────┴───────────┴─────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
        <BLANKLINE>
        (Showing first 2 of 2 rows)
    """
    from daft.ai._expressions import _PrompterExpression

    # Clean the Pydantic model to avoid Colab serialization issues
    if return_format is not None and IS_COLAB:
        return_format = clean_pydantic_model(return_format)

    # Load a PrompterDescriptor from the resolved provider
    # Pass return_format and system_message as explicit named arguments
    prompter_descriptor = _resolve_provider(provider, "openai").get_prompter(
        model,
        return_format=return_format,
        system_message=system_message,
        **options,
    )

    # Check if this is a vLLM provider - if so, use PyExpr.vllm directly
    from daft.ai.vllm.protocols.prompter import VLLMPrefixCachingPrompterDescriptor

    if isinstance(prompter_descriptor, VLLMPrefixCachingPrompterDescriptor):
        if return_format is not None:
            raise ValueError("return_format is not supported for vLLM provider")

        if system_message is not None:
            raise ValueError("system_message is not supported for vLLM provider")

        if isinstance(messages, list):
            raise ValueError("vLLM provider does not support multiple messages")

        vllm_options = prompter_descriptor.get_options()
        return Expression._from_pyexpr(
            messages._expr.vllm(
                prompter_descriptor.model_name,
                vllm_options["concurrency"],
                vllm_options["gpus_per_actor"],
                vllm_options["do_prefix_routing"],
                vllm_options["max_buffer_size"],
                vllm_options["min_bucket_size"],
                vllm_options["prefix_match_threshold"],
                vllm_options["load_balance_threshold"],
                vllm_options["batch_size"],
                vllm_options["engine_args"],
                vllm_options["generate_args"],
            )
        )

    # For non-vLLM providers, use the standard UDF-based execution path
    from daft.udf import method

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
    _PrompterExpression.__call__ = method(method=_PrompterExpression.prompt, return_dtype=return_dtype)  # type: ignore[method-assign]

    # Wrap the class with @daft.cls
    wrapped_cls = daft_cls(
        _PrompterExpression,
        gpus=udf_options.num_gpus or 0,
        max_concurrency=udf_options.concurrency,
        max_retries=udf_options.max_retries,
        on_error=udf_options.on_error,
    )

    # Instantiate the wrapped class with the prompter descriptor
    instance = wrapped_cls(prompter_descriptor)

    # Call the instance (which calls __call__ method) with the messages expression
    if isinstance(messages, list):
        return instance(*messages)
    else:
        return instance(messages)
