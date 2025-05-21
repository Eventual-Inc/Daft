# Expressions

Daft Expressions allow you to express some computation that needs to happen in a DataFrame. This page provides an overview of all the functionality that is provided by Daft Expressions. Learn more about [Expressions](../core_concepts.md#expressions) in Daft User Guide.

## Constructors

::: daft.expressions.col
    options:
        heading_level: 3

::: daft.expressions.lit
    options:
        heading_level: 3

::: daft.expressions.list_
    options:
        heading_level: 3

::: daft.expressions.struct
    options:
        heading_level: 3

::: daft.sql.sql.sql_expr
    options:
        heading_level: 3

<!--
## Generic
## Numeric
## Logical
## Aggregation
-->

::: daft.expressions.Expression
    options:
        filters: ["!^_[^_]", "!over", "!lag", "!lead"]

::: daft.expressions.expressions.ExpressionStringNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.str
        heading: Expression.str

::: daft.expressions.expressions.ExpressionBinaryNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.binary
        heading: Expression.binary

::: daft.expressions.expressions.ExpressionFloatNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.float
        heading: Expression.float

::: daft.expressions.expressions.ExpressionDatetimeNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.dt
        heading: Expression.dt

::: daft.expressions.expressions.ExpressionListNamespace
    options:
        filters: ["!^_", "!lengths"]
        toc_label: Expression.list
        heading: Expression.list

::: daft.expressions.expressions.ExpressionStructNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.struct
        heading: Expression.struct

::: daft.expressions.expressions.ExpressionMapNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.map
        heading: Expression.map

::: daft.expressions.expressions.ExpressionImageNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.image
        heading: Expression.image

::: daft.expressions.expressions.ExpressionPartitioningNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.partition
        heading: Expression.partition

::: daft.expressions.expressions.ExpressionUrlNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.url
        heading: Expression.url

::: daft.expressions.expressions.ExpressionJsonNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.json
        heading: Expression.json

::: daft.expressions.expressions.ExpressionEmbeddingNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.embedding
        heading: Expression.embedding

::: daft.expressions.visitor.ExpressionVisitor
    options:
        filters: ["!^_"]
