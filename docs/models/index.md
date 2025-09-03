# Models and Providers

This page contains references and guides for working with Models in daft.

## Implementing Model Expressions (Guide)

These steps will guide you through implementing a model expression like:

- `embed_text`
- `embed_image`
- `classify_text`

### Step 1. Define the Protocol and Descriptor

All model expressions are backed by a Protocol and Descriptor. These protocols are based on the verb and modality, and the
descriptor is used to instantiate the model at runtime.

```python
# daft.ai.protocols

@runtime_checkable
class TextClassifier(Protocol):
    """Protocol for text classification implementations."""

    def embed_text(self, text: list[str], labels: LabelLike | list[LabelLike]) -> list[Embedding]:
        """Classifies a batch of text strings using the given label(s)."""
        ...

class TextClassifierDescriptor(Descriptor[TextClassifier]):
    """Descriptor for a TextClassifier implementation."""
```

### Step 3. Add to the Provider Interface

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

### Step 4. Define the Function.

In `daft.functions.ai` you can add the function, and then re-export it in `daft.functions.__init__.py`.

```python
def classify_text(
  text: Expression,
  labels: LabelLike | list[LabelLike],
  *,
  provider: str | Provider | None = None,
  model: str | None = None,
) -> Expression: ...
```
