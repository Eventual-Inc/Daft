# Expressions

Daft Expressions allow you to express some computation that needs to happen in a DataFrame. This page provides an overview of all the functionality that is provided by Daft Expressions.

## Constructors

::: daft.expressions.col
    options:
        heading_level: 3

::: daft.expressions.lit
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

::: daft.expressions.WhenExpr
    options:
        filters: ["!^_"]

::: daft.expressions.visitor.ExpressionVisitor
    options:
        filters: ["!^_"]
