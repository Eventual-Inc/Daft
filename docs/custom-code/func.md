# Next-Generation UDFs with `@daft.func`

When Daft's built-in functions aren't sufficient for your needs, the `@daft.func` decorator lets you run your own Python code over each row of data. Simply decorate a Python function, and it becomes usable in Daft DataFrame operations.

!!! note "Active Development"
    `@daft.func` is currently in active development. While it works well for many use cases, some advanced features are still only available in the legacy [`@daft.udf`](udfs.md) decorator. See the [comparison section](#comparison-to-daftudf) below for details.

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

Daft automatically detects which variant to use based on your function signature:

- **Row-wise** (default): Regular Python functions process one row at a time
- **Async row-wise**: Async Python functions process rows concurrently
- **Generator**: Generator functions produce multiple output rows per input row

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

The newer `@daft.func` decorator provides a cleaner interface for many use cases, but has some limitations compared to the legacy `@daft.udf`:

| Feature | @daft.func | @daft.udf |
|---------|------------|-----------|
| Function UDFs | ✅ Yes | ✅ Yes |
| Type inference from hints | ✅ Yes | ❌ No |
| Eager evaluation mode | ✅ Yes | ❌ No |
| Async functions | ✅ Yes | ❌ No |
| Generator functions | ✅ Yes | ❌ No |
| Class UDFs | ❌ No | ✅ Yes |
| Concurrency control | ❌ No | ✅ Yes (class UDFs) |
| Resource requests (GPUs) | ❌ No | ✅ Yes |
| Multi-column batching | ❌ No | ✅ Yes |

If the new `@daft.func` decorator is missing a feature you need, we would love to hear from you! Please open an issue on our [GitHub repository](https://github.com/Eventual-Inc/Daft/issues).

See the [User-Defined Functions (UDFs)](udfs.md) documentation for details on `@daft.udf`.
