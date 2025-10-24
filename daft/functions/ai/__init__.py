"""AI Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

from daft import (
    DataType,
    Expression,
    Series,
    col,
    udf,
    current_session,
    current_provider,
)
from daft.ai.provider import Provider, ProviderType, load_provider, PROVIDERS

if TYPE_CHECKING:
    from pydantic import BaseModel
    from daft.ai.typing import Label

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

    Examples:
        >>> import daft
        >>> from daft.functions import embed_text
        >>> df = daft.from_pydict({"text": ["Hello World"]})
        >>> # Embed Text with Defaults
        >>> df = df.with_column(
        ...     "embeddings",
        ...     embed_text(
        ...         daft.col("text"),
        ...         provider="openai",  # Ensure OPENAI_API_KEY is set
        ...         model="text-embedding-3-small",
        ...     ),
        ... )
        >>> df.show()
        ╭─────────────┬──────────────────────────╮
        │ text        ┆ embeddings               │
        │ ---         ┆ ---                      │
        │ String      ┆ Embedding[Float32; 1536] │
        ╞═════════════╪══════════════════════════╡
        │ Hello World ┆ ▆█▆▆▆▃▆▆▂▄▃▂▃▃▄▁▃▅▂▃▂▂▂▂ │
        ╰─────────────┴──────────────────────────╯
        <BLANKLINE>
        (Showing first 1 of 1 rows)
    """
    from daft.ai._expressions import _TextEmbedderExpression
    from daft.ai.protocols import TextEmbedder

    # load a TextEmbedderDescriptor from the resolved provider
    text_embedder = _resolve_provider(provider, "transformers").get_text_embedder(model, **options)

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

    Examples:
        >>> import daft
        >>> from daft.functions import embed_image, decode_image
        >>> df = (
        ...     # Discover a few images from HuggingFace
        ...     daft.from_glob_path("hf://datasets/datasets-examples/doc-image-3/images")
        ...     # Read the 4 PNG, JPEG, TIFF, WEBP Images
        ...     .with_column("image_bytes", daft.col("path").url.download())
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
        text (String Expression):
            The input text column expression.
        labels (str | list[str]):
            Label(s) for classification.
        provider (str | Provider | None):
            The provider to use for the embedding model.
            By default this will use 'transformers' provider
        model (str | None):
            The embedding model to use. Can be a model instance or a model name.
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


def classify_image(
    image: Expression,
    labels: Label | list[Label],
    *,
    provider: str | Provider | None = None,
    model: str | None = None,
    **options: str,
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
            The embedding model to use. Can be a model instance or a model name.
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
        ...     .with_column("image_bytes", daft.col("path").url.download())
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
        ...             model="google/vit-base-patch16-224",
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
    from daft.ai.protocols import ImageClassifier

    image_classifier = _resolve_provider(provider, "transformers").get_image_classifier(model, **options)

    # TODO: classification with structured outputs will be more interesting
    label_list = [labels] if isinstance(labels, str) else labels

    # implemented as a class-based udf for now
    udf_options = image_classifier.get_udf_options()
    expr_callable = udf(
        return_dtype=DataType.string(),
        concurrency=udf_options.concurrency,
        num_gpus=udf_options.num_gpus,
    )

    expr = expr_callable(_ImageClassificationExpression)
    expr = expr.with_init_args(image_classifier, label_list)
    return expr(image)


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
    """Returns an expression that prompts a large language model using the specified model and provider.

    Args:
        messages (Expression): The input messages column expression.
        return_format (BaseModel | None): The return format for the prompt.
        system_message (str | None): The system message for the prompt.
        provider (str | Provider | None): The provider to use for the prompt.
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
        >>>     name="OpenRouter",
        >>>     base_url="https://openrouter.ai/api/v1",
        >>>     api_key=os.environ.get("OPENROUTER_API_KEY")
        >>> )
        ...
        >>> # Create a session and attach the provider
        >>> sess = Session()
        >>> sess.attach_provider(openrouter_provider)
        >>> sess.set_provider("OpenRouter")
        >>> # Create a dataframe with the quotes
        >>> df = daft.from_pydict({
        >>>     "quote": [
        >>>         "I am going to be the king of the pirates!",
        >>>         "I'm going to be the next Hokage!",
        >>>     ],
        >>> })
        ...
        >>> # Use the prompt function to classify the quotes
        >>> df = (
        >>>     df
        >>>     .with_column(
        >>>         "nemotron-response",
        >>>         prompt(
        >>>             daft.col("quote"),
        >>>             system_message="Classify the anime from the quote and return the show, character name, and explanation.",
        >>>             return_format=Anime,
        >>>             provider=sess.get_provider("OpenRouter"),
        >>>             model="nvidia/nemotron-nano-9b-v2:free"
        >>>         )
        >>>     )
        >>>     .select("quote", unnest(daft.col("nemotron-response")))
        >>> )
        ...
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
