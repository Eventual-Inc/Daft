# User-Defined Functions

User-Defined Functions (UDFs) are a mechanism to run Python code on the data that lives in a DataFrame. A UDF can be used just like [Expressions](expressions.md), allowing users to express computation that should be executed by Daft lazily.

To write a UDF, use the `@daft.func` or `@daft.cls` decorators to turn Python functions into Daft operations that work seamlessly with DataFrame expressions. Daft also supports custom aggregation functions, or UDAFs with `@daft.udaf` with a three-stage pipeline (aggregate, combine, finalize) that plugs into Daft's distributed aggregation engine.

Learn more about user-defined functions in the user guide: [`daft.func`](../custom-code/func.md), [`daft.cls`](../custom-code/cls.md), [`daft.udaf`](../custom-code/udaf.md).

## Stateless Function UDFs

::: daft.func

::: daft.udf._FuncDecorator

## Stateful Class UDFs

::: daft.cls

::: daft.method

::: daft.udf._MethodDecorator

::: daft.udf.udf_v2.Func

## Aggregate UDFs

::: daft.udaf
