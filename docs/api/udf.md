# User-Defined Functions

User-Defined Functions (UDFs) are a mechanism to run Python code on the data that lives in a DataFrame. A UDF can be used just like [Expressions](expressions.md), allowing users to express computation that should be executed by Daft lazily.

To write a UDF, you should use the `@udf` decorator, which can decorate either a Python function or a Python class, producing a UDF.

Learn more about [UDFs](../custom-code/udfs.md) in Daft User Guide.

## Creating UDFs

::: daft.udf.udf
    options:
        heading_level: 3

<!-- this function needs serious reformatting with the example and resource request section should not be a heading -->

## Using UDFs

::: daft.udf.UDF
    options:
        filters: ["!^_", "__call__"]

## `@daft.func` (Next-Generation UDFs)

`@daft.func` is a next-generation interface for creating user-defined functions (UDFs) in Daft. It provides a streamlined way to turn Python functions into Daft operations that work seamlessly with DataFrame expressions.

Learn more about [`@daft.func`](../custom-code/func.md) in Daft User Guide.

::: daft.func

::: daft.udf.row_wise.RowWiseUdf

::: daft.udf.generator.GeneratorUdf
