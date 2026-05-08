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

    def classify_text(self, text: list[str], labels: list[str]) -> list[str]:
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
will call the appropriate provider method to get the relevant descriptor.

```python
import daft
from daft import DataType, Series
from daft.ai.protocols import TextClassifier, TextClassifierDescriptor

def classify_text(
    text: Expression,
    labels: list[str],
    *,
    provider: str | Provider | None = None,
    model: str | None = None,
) -> Expression:
    # Load a TextClassifierDescriptor from the resolved provider
    text_classifier = _resolve_provider(provider, "transformers").get_text_classifier(model)

    # Create the stateful class UDF
    classifier = _TextClassifierExpression(text_classifier, labels)

    # Return the expression
    return classifier.classify(text)


@daft.cls
class _TextClassifierExpression:
    """Function expression implementation for a TextClassifier protocol."""

    def __init__(self, descriptor: TextClassifierDescriptor, labels: list[str]):
        # Instantiate from the descriptor in __init__
        self.text_classifier = descriptor.instantiate()
        self.labels = labels

    @daft.method.batch(return_dtype=DataType.string())
    def classify(self, text: Series) -> list[str]:
        text_list = text.to_pylist()
        if not text_list:
            return []
        return self.text_classifier.classify_text(text_list, self.labels)
```

## Step 4. Implement the Protocol for some Provider.

Here is a simplified example implementation of embed_text for OpenAI. This should give you
and idea of where you actual logic should live, and the previous steps are to properly
hook your new expression into the provider/model system.

```python
@dataclass
class OpenAITextEmbedderDescriptor(TextEmbedderDescriptor):
    model: str  # store some metadata

    # We can use the stored metadata to instantiate the protocol implementation
    def instantiate(self) -> TextEmbedder:
        return OpenAITextEmbedder(client=OpenAI(), model=self.model)

@dataclass
class OpenAITextEmbedder(TextEmbedder):
    client: OpenAI
    model: str

    # This is a simple version using the batch API. The full implementation
    # uses dynamic batching and has error handling mechanisms.
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
