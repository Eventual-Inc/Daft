# Running Custom Python Code in Daft

When Daft's built-in functions aren't sufficient for your needs, the `@daft.func` and `@daft.cls` decorators let you run your own Python code over each row of data. Simply decorate a Python function or class, and it becomes usable in Daft DataFrame operations.

If you are looking to migrate from Legacy UDFs to Daft's next generation, please see our [comparison guide](comparison.md).

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
- **Batch**: Process entire batches of data with `daft.Series` for high performance

Daft automatically detects which variant to use for regular functions based on your function signature. For batch functions, you must use the `@daft.func.batch` decorator.

## End to End Example with Return Type Inference



```python
import daft
import typing
import pydantic


class DoSomethingResultPydantic(pydantic.BaseModel):
    x3: int
    y3: int
    some_arg: str


class DoSomethingResultTypedDict(typing.TypedDict):
    x3: int
    y3: int
    some_arg: str


DoSomethingDaftDataType = daft.DataType.struct(
    {
        "x3": daft.DataType.int64(),
        "y3": daft.DataType.int64(),
        "some_arg": daft.DataType.string(),
    }
)


@daft.cls()
class NewUDFUsageExample:
    def __init__(self, x1: int, y1: int):
        self.x1 = x1
        self.y1 = y1

    # With Python Type Hint Resolution
    @daft.method()
    def do_something_typeddict(
        self, x2: int, y2: int, some_arg: str
    ) -> DoSomethingResultTypedDict:

        x3 = self.x1 + x2
        y3 = self.y1 - y2
        return {"x3": x3, "y3": y3, "some_arg": some_arg}

    @daft.method()
    def do_something_pydantic(
        self, x2: int, y2: int, some_arg: str
    ) -> DoSomethingResultPydantic:

        x3 = self.x1 + x2
        y3 = self.y1 - y2
        return DoSomethingResultPydantic(x3=x3, y3=y3, some_arg=some_arg)

    # With Return Dtype and no lint errors
    @daft.method(return_dtype=DoSomethingDaftDataType)
    def do_something_daft(self, x2: int, y2: int, some_arg: str):

        x3 = self.x1 * x2
        y3 = self.y1 // y2
        return {"x3": x3, "y3": y3, "some_arg": some_arg}


if __name__ == "__main__":
    # Instantiate the UDF
    my_udf = NewUDFUsageExample(x1=1, y1=2)

    # Create a dataframe
    df = daft.from_pydict({"x1": [1, 2, 3], "y1": [4, 5, 6]})

    # Use the UDF
    df = df.with_column(
        "something_typeddict",
        my_udf.do_something_typeddict(
            daft.col("x1"), daft.col("y1"), daft.lit("some_arg")
        ),
    )
    df = df.with_column(
        "something_pydantic",
        my_udf.do_something_pydantic(
            daft.col("x1"), daft.col("y1"), daft.lit("some_arg")
        ),
    )
    df = df.with_column(
        "something_daft",
        my_udf.do_something_daft(daft.col("x1"), daft.col("y1"), daft.lit("some_arg")),
    )
    df.show()
```
