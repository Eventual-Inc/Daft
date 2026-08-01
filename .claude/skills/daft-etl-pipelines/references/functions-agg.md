# functions agg

## any_value

```python
any_value(expr: Expression, ignore_nulls: bool=False) -> Expression
```

Returns any non-null value from the expression.

Args:
    expr (Expression): The input expression to select a value from.
    ignore_nulls: whether to ignore null values when selecting the value. Defaults to False.

## approx_count_distinct

```python
approx_count_distinct(expr: Expression) -> Expression
```

Calculates the approximate number of non-`NULL` distinct values in the expression.

Approximation is performed using the [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) algorithm.

## approx_percentiles

```python
approx_percentiles(expr: Expression, percentiles: float | list[float]) -> Expression
```

Calculates the approximate percentile(s) for a column of numeric values.

For numeric columns, we use the [sketches_ddsketch crate](https://docs.rs/sketches-ddsketch/latest/sketches_ddsketch/index.html).
This is a Rust implementation of the paper [DDSketch: A Fast and Fully-Mergeable Quantile Sketch with Relative-Error Guarantees (Masson et al.)](https://arxiv.org/pdf/1908.10693)

1. Null values are ignored in the computation of the percentiles
2. If all values are Null then the result will also be Null
3. If ``percentiles`` are supplied as a single float, then the resultant column is a ``Float64`` column
4. If ``percentiles`` is supplied as a list, then the resultant column is a ``FixedSizeList[Float64; N]`` column, where ``N`` is the length of the supplied list.

Args:
    percentiles: the percentile(s) at which to find approximate values at. Can be provided as a single
        float or a list of floats.

Returns:
    A new expression representing the approximate percentile(s). If `percentiles` was a single float, this will be a new `Float64` expression. If `percentiles` was a list of floats, this will be a new expression with type: `FixedSizeList[Float64, len(percentiles)]`.

## avg

```python
avg(expr: Expression) -> Expression
```

Calculates the mean of the values in the expression. Alias for mean().

## bool_and

```python
bool_and(expr: Expression) -> Expression
```

Calculates the boolean AND of all values in the expression.

For each group:
- Returns True if all non-null values are True
- Returns False if any non-null value is False
- Returns null if the group is empty or contains only null values

## bool_or

```python
bool_or(expr: Expression) -> Expression
```

Calculates the boolean OR of all values in the expression.

For each group:
- Returns True if any non-null value is True
- Returns False if all non-null values are False
- Returns null if the group is empty or contains only null values

## count

```python
count(expr: Expression | None=None, mode: Literal['all', 'valid', 'null'] | CountMode=CountMode.Valid) -> Expression
```

Counts the number of values in the expression.

Args:
    expr (Expression | None): The input expression to count values from. If not provided, mode must be "all"
        and count(*) semantics will be used.
    mode: A string ("all", "valid", or "null") that represents whether to count all values, non-null (valid) values, or null values. Defaults to "valid".

## count_distinct

```python
count_distinct(expr: Expression) -> Expression
```

Counts the number of distinct values in the expression.

## list_agg

```python
list_agg(expr: Expression) -> Expression
```

Aggregates the values in the expression into a list.

## list_agg_distinct

```python
list_agg_distinct(expr: Expression) -> Expression
```

Aggregates the values in the expression into a list of distinct values (ignoring nulls).

Returns:
    Expression: A List expression containing the distinct values from the input

## max

```python
max(expr: Expression) -> Expression
```

Calculates the maximum of the values in the expression.

## mean

```python
mean(expr: Expression) -> Expression
```

Calculates the mean of the values in the expression.

## median

```python
median(expr: Expression) -> Expression
```

Calculates the median of the values in the expression.

## min

```python
min(expr: Expression) -> Expression
```

Calculates the minimum of the values in the expression.

## percentile

```python
percentile(expr: Expression, percentage: float) -> Expression
```

Calculates the exact percentile for a column of numeric values.

Args:
    percentage: Percentage at which to compute the exact value. Must be between 0 and 1.

## product

```python
product(expr: Expression) -> Expression
```

Calculates the product of the values in the expression.

## skew

```python
skew(expr: Expression) -> Expression
```

Calculates the skewness of the values from the expression.

## stddev

```python
stddev(expr: Expression, ddof: int=1) -> Expression
```

Calculates the standard deviation of the values in the expression.

Args:
    expr: The input expression to calculate standard deviation for.
    ddof: Delta degrees of freedom. The divisor used in calculations
        is N - ddof, where N is the number of non-null elements.
        Defaults to 1 (sample standard deviation).

Returns:
    Expression representing the standard deviation.

## string_agg

```python
string_agg(expr: Expression, delimiter: str | None=None) -> Expression
```

Aggregates the values in the expression into a single string by concatenating them.

Args:
    delimiter: Optional delimiter to insert between concatenated values. Only supported for string columns.

## sum

```python
sum(expr: Expression) -> Expression
```

Calculates the sum of the values in the expression.

## var

```python
var(expr: Expression, ddof: int=1) -> Expression
```

Calculates the variance of the values in the expression.

Args:
    expr: The input expression to calculate variance for.
    ddof: Delta degrees of freedom. The divisor used in calculations
        is N - ddof, where N is the number of non-null elements.
        Defaults to 1 (sample variance).

Returns:
    Expression representing the variance.
