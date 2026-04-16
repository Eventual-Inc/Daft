# Stateless UDFs with `@daft.func`

When Daft's built-in functions aren't sufficient for your needs, the `@daft.func` and `@daft.cls` decorators let you run your own Python code over each row of data. Simply decorate a Python function or class, and it becomes usable in Daft DataFrame operations.

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

Use `max_concurrency` to limit the number of concurrent coroutines, for example to rate-limit API calls:

```python
@daft.func(max_concurrency=10)
async def fetch_url(url: str) -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()
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

## Resources, Concurrency, and Error Handling

`@daft.func`, `@daft.func.batch`, `@daft.cls`, and `@daft.method` share a common set of parameters for requesting resources, controlling concurrency, and handling errors. You do **not** need `@daft.cls` to request a GPU — it works on `@daft.func` too. Use `@daft.cls` only when you need to amortize expensive initialization (model load, connection setup) across rows.

| Parameter | Type | Default | Applies to |
|---|---|---|---|
| `cpus` | `float \| None` | `None` (engine decides) | `func`, `func.batch`, `cls` |
| `gpus` | `float` | `0` | `func`, `func.batch`, `cls` |
| `use_process` | `bool \| None` | `None` (auto) | `func`, `func.batch`, `cls` |
| `max_concurrency` | `int \| None` | `None` | `func` (async only), `func.batch` (async only), `cls` |
| `batch_size` | `int \| None` | `None` | `func.batch`, `method.batch` |
| `max_retries` | `int \| None` | `None` (no retries) | `func`, `func.batch`, `cls` |
| `on_error` | `"raise" \| "log" \| "ignore"` | `"raise"` | same as `max_retries` |
| `ray_options` | `dict[str, Any] \| None` | `None` | `func`, `func.batch`, `cls` |

### `max_concurrency`

`max_concurrency` controls two different things depending on where it's set:

- On `@daft.func` / `@daft.func.batch`: it caps the number of **concurrent coroutines** and only applies to `async` functions. Setting it on a sync function raises. For sync functions, reach for `@daft.cls` if you need actor-pool concurrency.
- On `@daft.cls`: it caps the number of **concurrent actor instances** for sync methods, or **concurrent coroutines** for async methods.

### `cpus` and `gpus`

Declare per-instance resource requests. The engine uses these for placement and (for `gpus`) for GPU isolation via `CUDA_VISIBLE_DEVICES`.

```python
# Single-GPU inference — no class needed if init is cheap
@daft.func(gpus=1)
def classify(image_bytes: bytes) -> str:
    import torch
    # model is loaded per-row here; prefer @daft.cls if init is expensive
    return run_model(image_bytes)
```

- `cpus` accepts fractional values (e.g. `0.5`).
- `gpus` accepts fractional values **up to 1.0** (e.g. `0.5` to pack two workers onto one GPU). Values above 1.0 must be integers.

If model initialization is expensive, prefer `@daft.cls` so the GPU-resident model is loaded once per worker. See the [GPU guide](gpu.md) for patterns.

### `use_process`

Runs each worker instance in a subprocess instead of a thread. Use this when your function is not thread-safe, holds the GIL heavily, or when you need hard isolation (e.g. a separate CUDA context per worker). The default (`None`) lets Daft pick at runtime based on observed performance.

### `max_retries` and `on_error`

Control what happens when a row raises an exception.

- `max_retries=N` retries failing calls up to `N` times with exponential backoff starting at 100 ms, doubling each attempt, capped at 60 s, with ±25% jitter. If the raised exception is a `daft.ai.utils.RetryAfterError`, the specified retry-after delay is honored instead of the default backoff.
- `on_error` decides what to do after retries are exhausted:
    - `"raise"` (default) — fail the query.
    - `"log"` — log the exception and emit `None` for that row.
    - `"ignore"` — silently emit `None` for that row.

```python
@daft.func(max_retries=3, on_error="log")
def call_flaky_api(url: str) -> str:
    import requests
    return requests.get(url).text
```

Both parameters work on sync, async, and batch variants. On `@daft.cls`, set them at the class level — they apply to every method.

!!! note "Per-method retry overrides"
    `@daft.method` and `@daft.method.batch` accept `max_retries` and `on_error` keyword arguments in their signatures, but those values are currently dropped: the class-level setting always wins. Until that's fixed, configure `max_retries` / `on_error` on the `@daft.cls` decorator. Tracked in [#6710](https://github.com/Eventual-Inc/Daft/issues/6710).

### `ray_options`

Forwarded to the Ray actor when running on the Ray runner (e.g. `{"resources": {"TPU": 1}}`, `{"runtime_env": {...}}`, `{"scheduling_strategy": ...}`). Setting `num_cpus`, `num_gpus`, or `memory` here raises an error — use the `cpus` / `gpus` parameters instead.

!!! warning "No memory-based placement on the new API"
    The new `@daft.func` / `@daft.cls` API has no parameter for memory-based placement: `cpus` / `gpus` are the only placement knobs today. `ray_options={"memory": ...}` is explicitly rejected, and the resulting error message refers to a `memory_bytes` argument that does not exist. If you relied on `memory_bytes` in the legacy `@daft.udf` API mostly to cap concurrency, use `max_concurrency` instead. Tracked in [#6711](https://github.com/Eventual-Inc/Daft/issues/6711).

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
