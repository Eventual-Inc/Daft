# Contributing New AI Functions

This page contains references and guides for developing new AI Functions in daft. These steps will guide you through implementing a model expression like:

- `embed_text`
- `embed_image`
- `classify_text`
- `prompt`

### Step 1. Define the Protocol and Descriptor

All model expressions are backed by a Protocol and Descriptor. These protocols are based on the verb and modality, and the
descriptor is used to instantiate the model at runtime.

```python
# daft.ai.protocols

@runtime_checkable
class TextClassifier(Protocol):
    """Protocol for text classification implementations."""

    def classify_text(self, text: list[str], labels: LabelLike | list[LabelLike]) -> list[Embedding]:
        """Classifies a batch of text strings using the given label(s)."""
        ...

class TextClassifierDescriptor(Descriptor[TextClassifier]):
    """Descriptor for a TextClassifier implementation."""
```

### Step 2. Add to the Provider Interface

You must update the Provider interface with a new method to create your descriptor. This should have
a default implementation which simply raises; this makes it so that you need not update all existing providers.

```python
# daft.ai.provider
class Provider(ABC):

    # ... existing code

    def get_text_classifier(self, model: str | None = None, **options: Any) -> TextClassifierDescriptor:
        """Returns a TextClassifierDescriptor for this provider."""
        raise not_implemented_err(self, method="classify_text")
```

### Step 3. Define the Function.

In `daft.functions.ai` you can add the function, and then re-export it in `daft.functions.__init__.py`.
The implementation is responsible for resolving the provider from the given arguments, then you
will call the appropriate provider method to get the relevant descriptor

```python
def classify_text(
    text: Expression,
    labels: LabelLike | list[LabelLike],
    *,
    provider: str | Provider | None = None,
    model: str | None = None,
) -> Expression:
    from daft.ai._expressions import _TextClassifierExpression
    from daft.ai.protocols import TextClassifier

    # Load a TextClassifierDescriptor from the resolved provider
    text_classifier = _resolve_provider(provider, "sentence_transformers").get_text_classifier(model, **options)

    # Implement the expression here!

    # This shows creating a class-based udf which holds state
    expr_udf = udf(
        return_dtype=get_type_from_labels(labels),
        concurrency=1,
        use_process=False,
    )

    # We invoke the UDF with a class callable to create an Expression
    expr = expr_udf(_TextClassifierExpression)   # <-- see step 4!
    expr = expr.with_init_args(text_classifier)

    # Now pass the input arguments to the expression!
    return expr(text, labels)


class _TextClassifierExpression:
    """Function expression implementation for a TextClassifier protocol."""

    text_classifier: TextClassifier

    def __init__(self, text_classifier: TextClassifierDescriptor):
        # !! IMPORTANT: instantiate from the descriptor in __init__ !!
        self.text_classifier = text_classifier.instantiate()

    def __call__(self, text_series: Series, labels: list[Label]) -> list[Embedding]:
        text = text_series.to_pylist()
        return self.text_classifier.classify_text(text, labels) if text else []
```

## Step 4. Implement the Protocol for some Provider.

Here is a simplified example implementation of embed_text for OpenAI. This should give you
and idea of where you actual logic should live, and the previous steps are to properly
hook your new expression into the provider/model system.

```python
dataclass
class OpenAITextEmbedderDescriptor(TextEmbedderDescriptor):
    model: str  # store some metadata

    # We can use the stored metadata to instantiate the protocol implementation
    def instantiate(self) -> TextEmbedder:
        return OpenAITextEmbedder(client=OpenAI(), model=self.model)

@dataclass
class OpenAITextEmbedder(TextEmbedder):
    client: OpenAI
    model: str

    # This is a a imple version using the batch API. The full implementation
    # is uses dynamic batching and has error handling mechanisms.
    def embed_text(self, text: list[str]) -> list[Embedding]:
        response = self.client.embeddings.create(
            input=text,
            model=self.model,
            encoding_format="float",
        )
        return [np.array(embedding.embedding) for embedding in response.data]
```

## Step 5. Expression Usage

You can now use this like any other expression.

```python
import daft

df = daft.read_parquet("/path/to/file.parquet")  # assuming has some column 'text'
df = df.with_column("embedding", embed_text(df["text"], provider="openai"))  # <- set provider to 'openai'
df.show()
```
