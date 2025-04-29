# Window Functions

Window functions allow you to perform calculations across a set of rows that are related to the current row. They operate on a group of rows (called a window frame) and return a result for each row based on the values in its window frame, without collapsing the result into a single row like aggregate functions do. Learn more about [Window Functions](../core_concepts.md/#window-functions) in the Daft User Guide.

## Window Specification

::: daft.window.Window

## Applying Window Functions

Window functions are applied to expressions using the [`.over()`][daft.expressions.Expression.over] method.

::: daft.expressions.Expression.over

## Ranking Functions

These functions compute ranks within a window partition. They require an [`order_by`][daft.window.Window.order_by] clause without a [`rows_between`][daft.window.Window.rows_between] or [`range_between`][daft.window.Window.range_between] clause in the window specification.

::: daft.functions.row_number

::: daft.functions.rank

::: daft.functions.dense_rank

## Lead/Lag Functions

These functions access data from preceding or succeeding rows within a window partition. They require an [`order_by`][daft.window.Window.order_by] clause without a [`rows_between`][daft.window.Window.rows_between] or [`range_between`][daft.window.Window.range_between] clause in the window specification.

::: daft.expressions.Expression.lag

::: daft.expressions.Expression.lead

## Aggregate Functions

Standard aggregate functions (e.g., [`sum()`][daft.expressions.Expression.sum], [`mean()`][daft.expressions.Expression.mean], [`count()`][daft.expressions.Expression.count], [`min()`][daft.expressions.Expression.min], [`max()`][daft.expressions.Expression.max], etc) can be used as window functions by applying them with [`.over()`][daft.expressions.Expression.over]. They work with all valid window specifications (partition by only, partition + order by, partition + order by + frame). Refer to the [Expressions API](./expressions.md) for a full list of aggregate functions.
