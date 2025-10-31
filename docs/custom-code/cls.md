## Stateful Class UDFs with `@daft.cls`

When your UDF requires expensive initialization such as loading a machine learning model, establishing database connections, or pre-computing lookup tables use `@daft.cls` to amortize the cost across multiple rows. The class is initialized once per worker, and the same instance processes all rows on that worker.

### Quick Example

```python
import daft

@daft.cls
class TextClassifier:
    def __init__(self, model_path: str):
        # This expensive initialization happens once per worker
        self.model = load_model(model_path)

    def __call__(self, text: str) -> str:
        return self.model.predict(text)

# Create an instance with initialization arguments
classifier = TextClassifier("path/to/model.pkl")

df = daft.from_pydict({"text": ["hello world", "goodbye world"]})

# Use the instance directly as a Daft function
df = df.select(classifier(df["text"]))
```

### How It Works

1. **Lazy Initialization**: When you create an instance like `classifier = TextClassifier("path/to/model.pkl")`, the `__init__` method is **not called immediately**. Instead, Daft saves the initialization arguments.

2. **Worker Initialization**: During query execution, Daft calls `__init__` on each instance with the saved arguments. Instances are reused for multiple rows.

3. **Method Calls**: Methods can be called with either:
   - **Expressions** (like `df["text"]`) - returns an Expression for DataFrame operations
   - **Scalars** (like `"hello"`) - executes immediately, initializing a local instance if needed

## Method Variants

Similarly to `daft.func`, Daft supports the same variants for `daft.method` to optimize for different use cases:

- **Row-wise** (default): Regular Python functions process one row at a time
- **Async row-wise**: Async Python functions process rows concurrently
- **Generator**: Generator functions produce multiple output rows per input row
- **Batch** (`@daft.method.batch`): Process entire batches of data with `daft.Series` for high performance

```python
@daft.cls
class Something:
    def __call__(self, x: float) -> float: ...
    def my_method(self, x: float) -> float: ...
    async def async_method(self, x: float) -> float: ...
    @daft.method.batch()
    def my_batch_method(self, s: Series) -> Series: ...
    @daft.method.batch()
    async def async_batch_method(self, s: Series) -> Series: ...
```

Daft automatically detects which variant to use for regular functions based on your function signature. For batch functions, you must use the `@daft.method.batch` decorator.


### Resource Control

Control computational resources with decorator parameters:

```python
@daft.cls(
    gpus=1,                    # Request 1 GPU per instance
    max_concurrency=4,         # Limit to 4 concurrent instances
    use_process=True           # Run in separate process
)
class ImageClassifier:
    def __init__(self, model_name: str):
        import torch
        self.model = torch.load(model_name).cuda()

    def __call__(self, image_path: str) -> str:
        image = load_image(image_path)
        return self.model(image)

classifier = ImageClassifier("resnet50.pth")
df = daft.from_pydict({"images": ["img1.jpg", "img2.jpg"]})
df = df.select(classifier(df["images"]))
```

**Parameters:**

- `gpus`: Number of GPUs required per instance (default: 0)
- `max_concurrency`: Maximum number of concurrent instances across all workers
- `use_process`: Whether to run in a separate process for isolation

### Using `@daft.method`

By default, all methods in a `@daft.cls` class can be used as Daft functions. Use the `@daft.method` decorator to override default behavior:

```python
import daft
from daft import DataType
from typing import Iterator

@daft.cls
class TextProcessor:
    def __init__(self, prefix: str):
        self.prefix = prefix

    # No decorator needed - works with default inference
    def __call__(self, text: str) -> str:
        return f"{self.prefix}{text}"

    # Override return type
    @daft.method(return_dtype=DataType.list(DataType.string()))
    def split_words(self, text: str):
        return text.split()

    # Unnest struct fields
    @daft.method(
        return_dtype=DataType.struct({
            "word_count": DataType.int64(),
            "char_count": DataType.int64()
        }),
        unnest=True
    )
    def analyze(self, text: str):
        words = text.split()
        return {
            "word_count": len(words),
            "char_count": len(text)
        }

processor = TextProcessor(">> ")
df = daft.from_pydict({"text": ["hello world", "foo bar"]})

df = df.select(
    processor(df["text"]).alias("prefixed"),  # Using __call__
    processor.split_words(df["text"]).alias("words"),
    processor.analyze(df["text"])  # Expands into word_count and char_count columns
)
```

### Method Variants

Like `@daft.func`, methods support multiple execution patterns:

#### Async Methods

```python
import aiohttp

@daft.cls
class APIClient:
    def __init__(self, api_key: str):
        self.api_key = api_key

    async def fetch_data(self, url: str) -> str:
        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {self.api_key}"}
            async with session.get(url, headers=headers) as response:
                return await response.text()

client = APIClient("my-secret-key")
df = daft.from_pydict({"urls": ["https://api.example.com/1", "https://api.example.com/2"]})
df = df.select(client.fetch_data(df["urls"]))
```

#### Generator Methods

```python
from typing import Iterator

@daft.cls
class TokenGenerator:
    def __init__(self, tokenizer_name: str):
        from transformers import AutoTokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(tokenizer_name)

    def tokenize(self, text: str) -> Iterator[str]:
        tokens = self.tokenizer.tokenize(text)
        for token in tokens:
            yield token

tokenizer = TokenGenerator("bert-base-uncased")
df = daft.from_pydict({"text": ["Hello world", "Daft is great"]})

# Each row produces multiple tokens
df = df.select("text", tokenizer.tokenize(df["text"]).alias("token"))
```

### Multiple Instances

You can create multiple instances of the same class with different configurations:

```python
@daft.cls
class Normalizer:
    def __init__(self, mean: float, std: float):
        self.mean = mean
        self.std = std

    def normalize(self, value: float) -> float:
        return (value - self.mean) / self.std

normalizer_a = Normalizer(mean=10.0, std=2.0)
normalizer_b = Normalizer(mean=50.0, std=5.0)

df = daft.from_pydict({
    "metric_a": [8, 10, 12],
    "metric_b": [45, 50, 55]
})

df = df.select(
    normalizer_a.normalize(df["metric_a"]).alias("norm_a"),
    normalizer_b.normalize(df["metric_b"]).alias("norm_b")
)
```

### Eager Execution

Call methods with scalar arguments to execute immediately:

```python
@daft.cls
class Calculator:
    def __init__(self, multiplier: int):
        self.multiplier = multiplier

    def __call__(self, x: int) -> int:
        return x * self.multiplier

calc = Calculator(10)

# Lazy execution - returns Expression
expr = calc(df["value"])

# Eager execution - initializes instance and returns result
result = calc(5)  # Returns 50
```

### Best Practices

1. **Costly Initialization**: Use `@daft.cls` when some an expensive initialization step can be reused across multiple rows (e.g., loading models, establishing connections). The initialization cost is amortized across all rows processed by each worker.
2. **Simple Functions**: Use `@daft.func` for operations that don't require expensive setup.
3. **Resource Management**: Request GPUs only when needed with the `gpus` parameter
4. **Concurrency**: Set `max_concurrency` to limit the number of concurrent instances.
5. **Process Isolation**: Use `use_process=True` to run each instance in a separate process. This is useful for isolating instances when they are not thread-safe or to improve performance by avoiding GIL contention.


### Batch Methods with `@daft.method.batch`

Similar to `@daft.func.batch`, use `@daft.method.batch` for batch processing in Daft classes:

```python
import daft
from daft import DataType, Series

@daft.cls
class Model:
    def __init__(self, model_name: str):
        self.model = load_model(model_name)

    @daft.method.batch(return_dtype=DataType.int64())
    def predict(self, x: Series) -> Series:
        predictions = self.model.predict(x.to_arrow().to_numpy())
        return predictions

model = Model("resnet50")
df = daft.from_pydict({"x": [1, 2, 3]})
df = df.select(model.predict(df["x"]))
```

### Batch Sizing

You can configure the maximum number of rows in each batch by setting the `batch_size` parameter:

```python
@daft.func.batch(return_dtype=DataType.int64(), batch_size=1024)
def batch_func_with_batch_size(x: Series) -> Series:
    assert len(x) <= 1024
    return x
```

Considerations for tuning batch size:

- **Avoiding out-of-memory errors**: Large batches can exhaust memory, especially with large rows (e.g., images, embeddings) or when using GPUs.
- **Improving performance**: The batch size for your function depends on your workload. Experiment to find the optimal size.
- **GPU utilization**: Match your batch size to your model's preferred inference batch size for best GPU performance.
