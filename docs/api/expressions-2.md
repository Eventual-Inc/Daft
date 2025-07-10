# Expressions

## Constructors

<!-- BEGIN GENERATED TABLE -->
| Expression | Description |
|------------|-------------|
| [`col`][daft.expressions.col] | Creates an Expression referring to the column with the provided name. |
| [`lit`][daft.expressions.lit] | Creates an Expression representing a column with every value set to the provided value. |
| [`list_`][daft.expressions.list_] | Constructs a list from the item expressions. |
| [`struct`][daft.expressions.struct] | Constructs a struct from the input field expressions. |
| [`interval`][daft.expressions.interval] | Creates an Expression representing an interval. |
| [`coalesce`][daft.expressions.coalesce] | Returns the first non-null value in a list of expressions. If all inputs are null, returns null. |
| [`sql_expr`][daft.sql.sql.sql_expr] | Parses a SQL string into a Daft Expression. |
<!-- END GENERATED TABLE -->

::: daft.expressions.col
::: daft.expressions.lit
::: daft.expressions.lit_
::: daft.expressions.struct
::: daft.expressions.interval
::: daft.expressions.coalesce
::: daft.sql.sql.sql_expr


## Generic

<!-- BEGIN GENERATED TABLE -->
| Expression | Description |
|------------|-------------|
| [`apply`][daft.expressions.expressions.Expression.apply] | Apply a function on each value in a given expression. |
<!-- END GENERATED TABLE -->

::: daft.expressions.expressions.Expression.apply


## URLs

::: daft.expressions.expressions.ExpressionUrlNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.url
        heading: Expression.url


## JSON

::: daft.expressions.expressions.ExpressionJsonNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.json
        heading: Expression.json


## Embedding

::: daft.expressions.expressions.ExpressionEmbeddingNamespace
    options:
        filters: ["!^_"]
        toc_label: Expression.embedding
        heading: Expression.embedding


## Visitor

::: daft.expressions.visitor.ExpressionVisitor
    options:
        filters: ["!^_"]
