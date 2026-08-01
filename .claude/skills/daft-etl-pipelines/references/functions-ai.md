# functions ai

## classify_image

```python
classify_image(image: Expression, labels: Label | list[Label], *, provider: str | Provider | None=None, model: str | None=None, **options: Unpack[ClassifyImageOptions]) -> Expression
```

Returns an expression that classifies images using the specified model and provider.

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

## classify_text

```python
classify_text(text: Expression, labels: Label | list[Label], *, provider: str | Provider | None=None, model: str | None=None, **options: Unpack[ClassifyTextOptions]) -> Expression
```

Returns an expression that classifies text using the specified model and provider.

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

## embed_image

```python
embed_image(image: Expression, *, provider: str | Provider | None=None, model: str | None=None, **options: Unpack[EmbedImageOptions]) -> Expression
```

Returns an expression that embeds images using the specified image model and provider.

Args:
    image (Image Expression): The input image column expression.
    provider (str | Provider | None): The provider to use for the image model. If None, the default provider is used.
    model (str | None): The image model to use. Can be a model instance or a model name. If None, the default model is used.
    **options: Any additional options to pass for the model.

Note:
    Make sure the required provider packages are installed (e.g. vllm, transformers, openai).

Returns:
    Expression (Embedding Expression): An expression representing the embedded image vectors.

## embed_text

```python
embed_text(text: Expression, *, provider: str | Provider | None=None, model: str | None=None, dimensions: int | None=None, **options: Unpack[EmbedTextOptions]) -> Expression
```

Returns an expression that embeds text using the specified embedding model and provider.

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

## prompt

```python
prompt(messages: list[Expression] | Expression, return_format: BaseModel | None=None, *, system_message: str | None=None, provider: str | Provider | None=None, model: str | None=None, **options: Any) -> Expression
```

Returns an expression that prompts a large language model using the specified model and provider.

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
