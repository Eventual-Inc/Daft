# Next-Generation UDFs with `@daft.func` and `@daft.cls`

When Daft's built-in functions aren't sufficient for your needs, the `@daft.func` and `@daft.cls` decorators let you run your own Python code over each row of data. Simply decorate a Python function or class, and it becomes usable in Daft DataFrame operations.

!!! note "Active Development"
    `@daft.func` and `@daft.cls` are currently in active development. While they work well for many use cases, some advanced features are still only available in the legacy [`@daft.udf`](udfs.md) decorator. See the [comparison section](#comparison-to-daftudf) below for details.

## Quick Example

```python
import daft

@daft.func
def add_and_format(a: int, b: int) -> str:
    return f"Sum: {a + b}"

df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
df = df.select(add_and_format(df["x"], df["y"]))
df.show()
```

```
╭───────────╮
│ x         │
│ ---       │
│ Utf8      │
╞═══════════╡
│ Sum: 5    │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ Sum: 7    │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ Sum: 9    │
╰───────────╯
```

## Function Variants

Daft supports multiple function variants to optimize for different use cases:

- **Row-wise** (default): Regular Python functions process one row at a time
- **Async row-wise**: Async Python functions process rows concurrently
- **Generator**: Generator functions produce multiple output rows per input row
- **Batch** (`@daft.func.batch`): Process entire batches of data with `daft.Series` for high performance

Daft automatically detects which variant to use for regular functions based on your function signature. For batch functions, you must use the `@daft.func.batch` decorator.

### Row-wise Functions

Row-wise functions are the default variant. They process one row at a time and return one value per row.

```python
import daft

@daft.func
def multiply(a: int, b: int) -> int:
    return a * b

df = daft.from_pydict({"x": [1, 2, 3], "y": [10, 20, 30]})
df = df.select(multiply(df["x"], df["y"]))
df.show()
```

```
╭───────╮
│ x     │
│ ---   │
│ Int64 │
╞═══════╡
│ 10    │
├╌╌╌╌╌╌╌┤
│ 40    │
├╌╌╌╌╌╌╌┤
│ 90    │
╰───────╯
```

#### Type Inference

Daft automatically infers the return type from your function's type hint:

```python
@daft.func
def tokenize(text: str) -> list[int]:
    vocab = {char: i for i, char in enumerate(set(text))}
    return [vocab[char] for char in text]

df = daft.from_pydict({"text": ["hello", "world"]})
df = df.select(tokenize(df["text"]))

# The return type is automatically inferred as List[Int64]
print(df.schema())
```

If you need to override the inferred type, use the `return_dtype` parameter:

```python
@daft.func(return_dtype=daft.DataType.int32())
def add(a: int, b: int) -> int:
    return a + b
```

#### Mixing Expressions and Literals

You can mix DataFrame expressions with literal values:

```python
@daft.func
def add_constant(value: int, constant: int) -> int:
    return value + constant

df = daft.from_pydict({"x": [1, 2, 3]})
df = df.select(add_constant(df["x"], 100))  # constant is a literal
df.show()
```

```
╭───────╮
│ x     │
│ ---   │
│ Int64 │
╞═══════╡
│ 101   │
├╌╌╌╌╌╌╌┤
│ 102   │
├╌╌╌╌╌╌╌┤
│ 103   │
╰───────╯
```

#### Keyword Arguments

Functions with default arguments work as expected:

```python
@daft.func
def format_number(value: int, prefix: str = "$", suffix: str = "") -> str:
    return f"{prefix}{value}{suffix}"

df = daft.from_pydict({"amount": [10, 20, 30]})

# Use defaults
df.select(format_number(df["amount"])).show()

# Override with literals
df.select(format_number(df["amount"], prefix="€", suffix=" EUR")).show()

# Override with expressions
df.select(format_number(df["amount"], suffix=df["amount"].cast(daft.DataType.string()))).show()
```

#### Eager Evaluation

When called without any expressions, functions execute immediately:

```python
@daft.func
def add(a: int, b: int) -> int:
    return a + b

# This executes immediately and returns 8
result = add(3, 5)
print(result)  # 8

# This returns a Daft Expression
expr = add(df["x"], df["y"])
```

### Async Row-wise Functions

Decorate async functions to enable concurrent execution across rows:

```python
import daft
import asyncio
import aiohttp

@daft.func
async def fetch_url(url: str) -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()

df = daft.from_pydict({
    "urls": [
        "https://api.example.com/1",
        "https://api.example.com/2",
        "https://api.example.com/3",
    ]
})

# Requests are made concurrently
df = df.select(fetch_url(df["urls"]))
```

### Generator Functions

Generator functions use `yield` to produce multiple output rows per input row. Other columns in the DataFrame are automatically broadcast to match the number of generated values. You may only use one generator function per DataFrame operation.

```python
import daft
from typing import Iterator

@daft.func
def repeat_value(value: str, count: int) -> Iterator[str]:
    for _ in range(count):
        yield value

df = daft.from_pydict({
    "id": [1, 2, 3],
    "word": ["hello", "world", "daft"],
    "times": [2, 3, 1]
})

df = df.select("id", repeat_value(df["word"], df["times"]))
df.show()
```

```
╭───────┬───────╮
│ id    ┆ word  │
│ ---   ┆ ---   │
│ Int64 ┆ Utf8  │
╞═══════╪═══════╡
│ 1     ┆ hello │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 1     ┆ hello │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 2     ┆ world │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 2     ┆ world │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 2     ┆ world │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 3     ┆ daft  │
╰───────┴───────╯
```

Notice how the `id` column values are repeated to match the number of generated values.

#### Type Hints for Generators

Use `Iterator[T]` or `Generator[T, None, None]` type hints to indicate the yielded type:

```python
from typing import Iterator

@daft.func
def split_text(text: str) -> Iterator[str]:
    for word in text.split():
        yield word
```

Alternatively, specify the return type explicitly:

```python
@daft.func(return_dtype=daft.DataType.string())
def split_text(text: str):
    for word in text.split():
        yield word
```

#### Handling Empty Generators

If a generator yields no values for a particular input row, a null value is inserted:

```python
@daft.func
def yield_if_positive(value: int) -> Iterator[int]:
    if value > 0:
        yield value

df = daft.from_pydict({"x": [-1, 0, 5, 10]})
df = df.select(yield_if_positive(df["x"]))
df.show()
```

```
╭───────╮
│ x     │
│ ---   │
│ Int64 │
╞═══════╡
│ None  │
├╌╌╌╌╌╌╌┤
│ None  │
├╌╌╌╌╌╌╌┤
│ 5     │
├╌╌╌╌╌╌╌┤
│ 10    │
╰───────╯
```

## Class UDFs with `@daft.cls`

When your UDF requires expensive initialization—such as loading a machine learning model, establishing database connections, or pre-computing lookup tables—use `@daft.cls` to amortize the cost across multiple rows. The class is initialized once per worker, and the same instance processes all rows on that worker.

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

## Batch UDFs with `@daft.func.batch`

For performance-critical operations, batch UDFs process entire batches of data at once using `daft.Series` objects instead of individual values. This allows you to leverage optimized libraries like PyArrow or NumPy for efficient vectorized operations.

### When to Use Batch UDFs

Use batch UDFs when:

- **Performance is critical**: Vectorized operations may be significantly faster than row-wise processing
- **Working with optimized libraries**: You want to use PyArrow compute functions, NumPy operations, or other libraries that support vectorized operations on batch data
- **Running batch inference**: You are running a model that supports batch inference

### Basic Batch UDF

Batch UDFs receive `daft.Series` objects as arguments and return a `daft.Series`, `list`, `numpy.ndarray`, or `pyarrow.Array`:

```python
import daft
from daft import DataType, Series

@daft.func.batch(return_dtype=DataType.int64())
def add_series(a: Series, b: Series) -> Series:
    import pyarrow.compute as pc

    # Convert to PyArrow for efficient computation
    a_arrow = a.to_arrow()
    b_arrow = b.to_arrow()
    result = pc.add(a_arrow, b_arrow)

    return result

df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
df = df.select(add_series(df["x"], df["y"]))
df.show()
```

```
╭───────╮
│ x     │
│ ---   │
│ Int64 │
╞═══════╡
│ 5     │
├╌╌╌╌╌╌╌┤
│ 7     │
├╌╌╌╌╌╌╌┤
│ 9     │
╰───────╯
```

### Mixing Series and Scalar Arguments

Batch UDFs can accept both Series and scalar arguments. Scalar arguments are passed through without modification:

```python
@daft.func.batch(return_dtype=DataType.int64())
def add_constant(a: Series, constant: int) -> Series:
    import pyarrow.compute as pc

    a_arrow = a.to_arrow()
    result = pc.add(a_arrow, constant)
    return result

df = daft.from_pydict({"x": [1, 2, 3]})
df = df.select(add_constant(df["x"], 100))
df.show()
```

```
╭───────╮
│ x     │
│ ---   │
│ Int64 │
╞═══════╡
│ 101   │
├╌╌╌╌╌╌╌┤
│ 102   │
├╌╌╌╌╌╌╌┤
│ 103   │
╰───────╯
```

### Eager Evaluation

Like regular functions, batch UDFs execute immediately when called with scalars:

```python
@daft.func.batch(return_dtype=DataType.int64())
def multiply(a: Series, b: Series) -> Series:
    import pyarrow.compute as pc

    a_arrow = a.to_arrow()
    b_arrow = b.to_arrow()
    result = pc.multiply(a_arrow, b_arrow)
    return result

# Lazy execution - returns Expression
expr = multiply(df["x"], df["y"])

# Eager execution - computes immediately
result = multiply(5, 10)  # Returns 50
```

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

## Advanced Features

### Unnesting Struct Returns

When your function returns a struct (dictionary), you can use `unnest=True` to automatically expand the struct fields into separate columns:

```python
import daft

@daft.func(
    return_dtype=daft.DataType.struct({
        "first": daft.DataType.string(),
        "last": daft.DataType.string(),
        "age": daft.DataType.int64()
    }),
    unnest=True
)
def parse_person(full_name: str, age: int):
    parts = full_name.split()
    return {"first": parts[0], "last": parts[1], "age": age}

df = daft.from_pydict({
    "name": ["Alice Smith", "Bob Jones"],
    "age": [30, 25]
})

df = df.select(parse_person(df["name"], df["age"]))
df.show()
```

```
╭───────┬───────┬───────╮
│ first ┆ last  ┆ age   │
│ ---   ┆ ---   ┆ ---   │
│ Utf8  ┆ Utf8  ┆ Int64 │
╞═══════╪═══════╪═══════╡
│ Alice ┆ Smith ┆ 30    │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ Bob   ┆ Jones ┆ 25    │
╰───────┴───────┴───────╯
```

Without `unnest=True`, you would get a single column containing struct values.

### Combining Generators with Unnest

You can combine generator functions with `unnest=True` to yield multiple structs that get expanded into columns:

```python
from typing import Iterator

@daft.func(
    return_dtype=daft.DataType.struct({
        "index": daft.DataType.int64(),
        "char": daft.DataType.string()
    }),
    unnest=True
)
def enumerate_chars(text: str) -> Iterator[dict]:
    for i, char in enumerate(text):
        yield {"index": i, "char": char}

df = daft.from_pydict({"word": ["hi", "bye"]})
df = df.select(enumerate_chars(df["word"]))
df.show()
```

```
╭───────┬──────╮
│ index ┆ char │
│ ---   ┆ ---  │
│ Int64 ┆ Utf8 │
╞═══════╪══════╡
│ 0     ┆ h    │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ 1     ┆ i    │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ 0     ┆ b    │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ 1     ┆ y    │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ 2     ┆ e    │
╰───────┴──────╯
```

## Comparison to @daft.udf

The newer `@daft.func` and `@daft.cls` decorators provide a cleaner interface for most use cases. The legacy `@daft.udf` decorator still has a few advanced features:

| Feature | @daft.func / @daft.cls | @daft.udf |
|---------|------------------------|-----------|
| Function UDFs | ✅ Yes (@daft.func) | ✅ Yes |
| Class UDFs | ✅ Yes (@daft.cls) | ✅ Yes |
| Type inference from hints | ✅ Yes | ❌ No |
| Eager evaluation mode | ✅ Yes | ❌ No |
| Async functions | ✅ Yes | ❌ No |
| Generator functions | ✅ Yes | ❌ No |
| Concurrency control | ✅ Yes (@daft.cls) | ✅ Yes (class UDFs) |
| Resource requests (GPUs) | ✅ Yes (@daft.cls) | ✅ Yes |
| Batch processing | ✅ Yes (@daft.func.batch) | ✅ Yes |

If the new decorators are missing a feature you need, we would love to hear from you! Please open an issue on our [GitHub repository](https://github.com/Eventual-Inc/Daft/issues).

See the [User-Defined Functions (UDFs)](udfs.md) documentation for details on `@daft.udf`.
