# functions window

## dense_rank

```python
dense_rank() -> Expression
```

Return the dense rank of the current row (used for window functions).

The dense rank is the rank of the current row without gaps.

## first_value

```python
first_value(expr: Expression, ignore_nulls: bool=False) -> Expression
```

Returns the first value in the window frame.

Must be used with ``over()`` to specify the window partition, order, and frame.
When ``ignore_nulls=True``, skips null values and returns the first non-null value.

Args:
    expr (Expression): The input expression.
    ignore_nulls: whether to ignore null values. Defaults to False.

Returns:
    Expression: The first value in the window frame.

## lag

```python
lag(expr: Expression, offset: int=1, default: Expression | None=None) -> Expression
```

Get the value from a previous row within a window partition.

Args:
    expr: The expression to get the lagged value of.
    offset: The number of rows to shift backward. Must be >= 0.
    default: Value to use when no previous row exists. Can be a column reference.

Returns:
    Expression: Value from the row `offset` positions before the current row.

## last_value

```python
last_value(expr: Expression, ignore_nulls: bool=False) -> Expression
```

Returns the last value in the window frame.

Must be used with ``over()`` to specify the window partition, order, and frame.
When ``ignore_nulls=True``, skips null values and returns the last non-null value.

Args:
    expr (Expression): The input expression.
    ignore_nulls: whether to ignore null values. Defaults to False.

Returns:
    Expression: The last value in the window frame.

## lead

```python
lead(expr: Expression, offset: int=1, default: Expression | None=None) -> Expression
```

Get the value from a future row within a window partition.

Args:
    expr: The expression to get the lead value of.
    offset: The number of rows to shift forward. Must be >= 0.
    default: Value to use when no future row exists. Can be a column reference.

Returns:
    Expression: Value from the row `offset` positions after the current row.

## over

```python
over(expr: Expression, window: Window) -> Expression
```

Apply the expression as a window function.

Args:
    expr: The expression to apply as a window function.
    window: The window specification (created using ``daft.Window``)
        defining partitioning, ordering, and framing.

## rank

```python
rank() -> Expression
```

Return the rank of the current row (used for window functions).

## row_number

```python
row_number() -> Expression
```

Return the row number of the current row (used for window functions).
