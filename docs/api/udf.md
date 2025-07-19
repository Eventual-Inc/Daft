# User-Defined Functions

User-Defined Functions (UDFs) are a mechanism to run Python code on the data that lives in a DataFrame. A UDF can be used just like [Expressions](expressions.md), allowing users to express computation that should be executed by Daft lazily.

To write a UDF, you should use the `@udf` decorator, which can decorate either a Python function or a Python class, producing a UDF. Learn more about [UDFs](../core_concepts.md#user-defined-functions-udf) in Daft User Guide.

## Creating UDFs

::: daft.udf.udf

## Using UDFs

::: daft.udf.UDF
    options:
        filters: ["!^_", "__call__"]
        heading_level: 3
