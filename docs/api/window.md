# Window Functions

Window functions allow you to perform calculations across a set of rows that are related to the current row. They operate on a group of rows (called a window frame) and return a result for each row based on the values in its window frame, without collapsing the result into a single row like aggregate functions do.

## Window Specification

Define the window partitioning, ordering, and framing using the `Window` class.

::: daft.window.Window
    options:
        heading_level: 3
        show_root_toc_entry: false
        show_if_no_docstring: true
        members:
          - partition_by
          - order_by
          - rows_between
          - range_between

## Applying Window Functions

Window functions are applied to expressions using the `.over()` method.

::: daft.expressions.Expression.over
    options:
        heading_level: 3
        show_root_toc_entry: false
        show_if_no_docstring: true

## Ranking Functions

These functions compute ranks within a window partition. They require an `order_by` clause without a `rows_between` or `range_between` clause in the window specification.

::: daft.functions.row_number
    options:
        heading_level: 3
        show_root_toc_entry: false
        show_if_no_docstring: true

::: daft.functions.rank
    options:
        heading_level: 3
        show_root_toc_entry: false
        show_if_no_docstring: true

::: daft.functions.dense_rank
    options:
        heading_level: 3
        show_root_toc_entry: false
        show_if_no_docstring: true

## Lead/Lag Functions

These functions access data from preceding or succeeding rows within a window partition. They require an `order_by` clause without a `rows_between` or `range_between` clause in the window specification.

::: daft.expressions.Expression.lag
    options:
        heading_level: 3
        show_root_toc_entry: false
        show_if_no_docstring: true

::: daft.expressions.Expression.lead
    options:
        heading_level: 3
        show_root_toc_entry: false
        show_if_no_docstring: true

## Aggregate Functions

Standard aggregate functions (e.g., `sum`, `mean`, `min`, `max`, `count`) can be used as window functions by applying them with `.over()`. They work with all valid window specifications (partition by only, partition + order by, partition + order by + frame). Refer to the [Expressions API](./expressions.md) for a full list of aggregate functions.
