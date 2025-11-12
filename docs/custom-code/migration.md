# Migrating to Daft's new UDF API

Daft now offers a new UDF API via the `@daft.func` and `@daft.cls` decorators, replacing the legacy `@daft.udf` decorator. The new API is more powerful and Pythonic, and the legacy API will be deprecated in a future release.

This guide will walk you through the steps to migrate your existing UDFs to the new API.

## Function UDF

Python functions decorated with `@daft.udf` can be easily converted into a batch function using the `@daft.func.batch` decorator.

```python hl_lines="7"
# ====== Legacy API ======
@daft.udf(...)
def my_func(a: daft.Series, b: daft.Series):
    ...

# ====== New API ======
@daft.func.batch(...)
def my_func(a: daft.Series, b: daft.Series):
    ...
```

Then, they can be used exactly the same as before.

Just like with legacy UDFs, inputs to batch functions are `daft.Series` objects, and the same return types are supported as well: `daft.Series`, `list`, `numpy.ndarray`, or `pyarrow.Array`.

Most decorator parameters also have equivalent parameters in the legacy UDF decorator. See the [Decorator Parameters](#decorator-parameters) section for more details.

## Class UDF

### Creation
Python classes decorated with `@daft.udf` can be easily converted into a class UDF with a batch method using the `@daft.cls` and `@daft.method.batch` decorators.

```python hl_lines="11 16"
# ====== Legacy API ======
@daft.udf(...)
class MyClass:
    def __init__(self):
        ...

    def __call__(self, value: daft.Series):
        ...

# ====== New API ======
@daft.cls(...)
class MyClass:
    def __init__(self):
        ...

    @daft.method.batch(...)
    def __call__(self, value: daft.Series):
        ...
```

### Usage

Unlike legacy class UDFs, which can be called directly as a function, for new class UDFs, you must first create an instance of the class and then call the method on the instance.

```python hl_lines="7 9"
# ====== Legacy API ======
df.select(
    MyClass(df["value"])
)

# ====== New API ======
my_instance = MyClass()
df.select(
    my_instance(df["value"])
)
```

Instead of using `.with_init_args(...)` to specify the arguments to the `__init__` method, you can set those arguments in the construction of the class instance instead.

```python  hl_lines="8 10"
# ====== Legacy API ======
MyClassWithArgs = MyClass.with_init_args(arg1=1, arg2=2)
df.select(
    MyClassWithArgs(df["value"])
)

# ====== New API ======
my_instance_with_args = MyClass(arg1=1, arg2=2)
df.select(
    my_instance_with_args(df["value"])
)
```

!!! note Class Methods
    Whereas legacy class UDFs require the implementation of the `__call__` method, new class UDFs allow you to implement other methods in the class and use them as UDFs.

    For example, here is a class with a `generate` method that can be used as a UDF:
    ```python
    @daft.cls(...)
    class MyClass:
        def __init__(self):
            ...

        def generate(self, value: daft.Series):
            ...
    ```

    Then, you can use the `generate` method as follows:
    ```python
    my_instance = MyClass()
    df.select(
        my_instance.generate(df["value"])
    )
    ```

## Decorator Parameters

The following parameters stay the same between the legacy and new APIs:

- **return_dtype**
- **batch_size**
- **use_process**

For these parameters, here's what you can do with the new API:

- **concurrency**: The new API offers a `max_concurrency` parameter instead, which guarantees that at most `max_concurrency` instances of the UDF will be running at any given time, instead of exactly `concurrency` instances.
- **num_gpus**: The new API offers a `gpus` parameter which has the same effect as `num_gpus`. This parameter is supported in `@daft.cls`.
- **num_cpus**: The new API currently does not have an equivalent parameter for `num_cpus`. If you are using `num_cpus` to limit the number of instances of the UDF that can run at any given time, consider using `max_concurrency` instead.
- **memory_bytes**: The new API currently does not have an equivalent parameter for `memory_bytes`. If you are using `memory_bytes` to limit the number of instances of the UDF that can run at any given time, consider using `max_concurrency` instead.

## New Features

Here are some features that are available in the new API that may help simplify your code and improve performance during your migration.

See the main [Function UDF](./func.md) and [Class UDF](./cls.md) pages for a detailed description of the new API.

### Row-wise Functions

If you find that your UDF is simply iterating over the rows of the input data and computing a result for each row without any vectorized or batch operations, consider implementing it as a row-wise function using the `@daft.func` decorator.

Row-wise functions receive a single row of input data at a time, and return a single value for that row. Daft will automatically handle batching and conversion between Daft and Python types under the hood.

Example:
```python
# ====== Legacy Batch API ======
@daft.udf(return_dtype=daft.DataType.int64())
def my_sum(a: daft.Series, b: daft.Series):
    result = []
    for a_val, b_val in zip(a, b):
        result.append(a_val + b_val)
    return result

# ====== New Row-wise API ======
@daft.func
def my_sum(a: int, b: int) -> int:
    return a + b
```

### Return Type Inference

With row-wise functions, Daft will also automatically infer the return type of the function based on the Python type annotations. For example, in the above example, by specifying `-> int`, Daft will automatically infer the return dtype to be `daft.DataType.int64()`.

The `return_dtype` parameter is still supported, but it is not required. See the [Type Conversions](../../api/datatypes/type_conversions#python-to-daft) page for a mapping from Python types to Daft types.

### Async Functions

The new UDF API supports async Python functions natively for both row-wise and batch functions. Simply specify the `async` keyword in front of the function definition and use it like a regular Daft function.

Daft will handle the asynchronous execution of the function under the hood, so if you are calling to async functions from within your UDF, you can now just `await` them directly.

Example:
```python
@daft.func
def my_api_call(prompt: str) -> str:
    response = await make_api_call(prompt)
    return response.text
```

## Known Limitations

- Specifying granular CPU and memory resource requests is not yet supported in the new API. We found that the behavior of the `num_cpus` and `memory_bytes` parameters in the legacy API were unclear, and that they were largely used to control the concurrency of the UDF. In those cases, consider using `max_concurrency` instead.

- There is currently no method to control the number of concurrent calls to async UDFs in the new API.

If you have any questions or feedback about the new UDF API, please submit an [issue on GitHub](https://github.com/Eventual-Inc/Daft/issues) or reach out to us on [Slack](https://join.slack.com/t/dist-data/shared_invite/zt-2e77olvxw-uyZcPPV1SRchhi8ah6ZCtg).
