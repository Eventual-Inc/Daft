# User-Defined Functions

User-Defined Functions (UDFs) are a mechanism to run Python code on the data that lives in a DataFrame. A UDF can be used just like [Expressions](expressions.md), allowing users to express computation that should be executed by Daft lazily.

To write a UDF, you should use `@daft.func` or `@daft.cls`, which provide a streamlined way to turn Python functions into Daft operations that work seamlessly with DataFrame expressions.

Learn more about UDFs in the [User Guide](../custom-code/func.md).

## Function UDFs

`@daft.func` and `@daft.cls` are the recommended interface for creating user-defined functions in Daft.

Learn more in the [User Guide](../custom-code/func.md).

::: daft.func

::: daft.udf._FuncDecorator

::: daft.cls

::: daft.method

::: daft.udf._MethodDecorator

::: daft.udf.udf_v2.Func

## Aggregate UDFs

`@daft.udaf` lets you define custom aggregation functions with a three-stage pipeline (aggregate, combine, finalize) that plugs into Daft's distributed aggregation engine.

Learn more in the [User Guide](../custom-code/udaf.md).

::: daft.udaf

## Legacy UDFs

!!! danger "To Be Removed in 0.8.0"

    The `@daft.udf` decorator has been **deprecated** since Daft 0.7.0 and **will be removed in 0.8.0**.
    Please use `@daft.func` and `@daft.cls` instead.

    Using `@daft.udf` emits the following warning:

    ```
    DeprecationWarning: The `@daft.udf` decorator is deprecated since Daft version >= 0.7.0
    and will be removed in >= 0.8.0. Please use `@daft.func` and `@daft.cls` instead.
    See the migration guide for more details:
    https://docs.daft.ai/en/stable/custom-code/migration/
    ```

    See the [migration guide](../custom-code/migration.md) for how to update your code.

### Creating Legacy UDFs

::: daft.udf.udf
    options:
        heading_level: 4

### Using Legacy UDFs

::: daft.udf.UDF
    options:
        filters: ["!^_", "__call__"]
