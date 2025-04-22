# Window Functions

Window functions allow you to perform calculations across a set of rows that are related to the current row. They operate on a group of rows (called a window frame) and return a result for each row based on the values in its window frame, without collapsing the result into a single row like aggregate functions do.

## Window Class

The `Window` class is used to specify the window definition for window functions.

```python
from daft import Window
```

### Basic Configuration

A window definition can be created and configured using the following methods:

```python
# Create a window specification
window_spec = Window().partition_by("category")
```

### Available Methods

#### partition_by

```python
Window().partition_by(cols)
```

Partitions the data based on one or more columns or expressions.

**Arguments:**

- `cols`: Columns or expressions on which to partition data. Can be column names as strings, Expression objects, or iterables of these.

**Returns:**
- A `Window` object with the specified partitioning.

**Example:**

```python
# Single column partition
window_spec = Window().partition_by("category")

# Multiple column partition
window_spec = Window().partition_by(["region", "category"])
```

#### order_by

```python
Window().partition_by(cols).order_by(cols, desc=False)
```

Orders rows within each partition by specified columns or expressions.

**Arguments:**

- `cols`: Columns or expressions to determine ordering within the partition.
- `desc`: Sort descending (True) or ascending (False). Default is False (ascending).

**Returns:**
- A `Window` object with the specified partitioning and ordering.

**Note:** NULL values are positioned at the end for ascending order (default) and at the beginning for descending order.

**Example:**

```python
# Order by a single column (ascending, NULLs last)
window_spec = Window().partition_by("category").order_by("date")

# Order by a single column (descending, NULLs first)
window_spec = Window().partition_by("category").order_by("sales", desc=True)

# Order by multiple columns with different sort orders
window_spec = Window().partition_by("category").order_by(
    ["date", "sales"], desc=[False, True]
)
```

#### rows_between

```python
Window().partition_by(cols).order_by(cols).rows_between(start, end, min_periods=1)
```

Restricts each window to a row-based frame between start and end boundaries.

**Arguments:**

- `start`: Boundary definition for the start of the window. Can be:
  - `Window.unbounded_preceding`: All rows before the current row
  - `Window.current_row`: The current row
  - Integer value (e.g. `3`): Number of rows preceding the current row
- `end`: Boundary definition for the end of the window. Can be:
  - `Window.unbounded_following`: All rows after the current row
  - `Window.current_row`: The current row
  - Integer value (e.g. `3`): Number of rows following the current row
- `min_periods`: Minimum number of rows required in the window frame to compute a result (default = 1). When there are fewer rows than `min_periods` within a window frame, the result will be `NULL`.

**Returns:**
- A `Window` object with the specified frame bounds.

**Example:**

```python
# Running sum (all preceding rows and current row)
running_window = Window().partition_by("category").order_by("date").rows_between(
    Window.unbounded_preceding, Window.current_row
)

# Moving average (2 preceding rows, current row, and 2 following rows)
moving_window = Window().partition_by("category").order_by("date").rows_between(
    -2, 2
)

# Moving average requiring at least 3 rows in the frame
moving_window_min_periods = Window().partition_by("category").order_by("date").rows_between(
    -2, 2, min_periods=3
)
```

### Understanding min_periods

The `min_periods` parameter specifies the minimum number of rows that must be present in the window frame for a calculation to be performed. If fewer rows exist in the frame, the function returns NULL.

#### Example with min_periods

Consider a time series dataset:

```python
from daft import Window, col

# Sample data
df = daft.from_pydict({
    "date": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05"],
    "value": [10.0, 20.0, 30.0, 40.0, 50.0]
})

# Default min_periods=1 (will compute result even with just one row in the frame)
window1 = Window().order_by("date").rows_between(-1, 1, min_periods=1)
# Requiring at least 2 rows in the frame
window2 = Window().order_by("date").rows_between(-1, 1, min_periods=2)
# Requiring at least 3 rows in the frame
window3 = Window().order_by("date").rows_between(-1, 1, min_periods=3)

# Compare results with different min_periods settings
df = df.with_columns({
    "avg_min1": col("value").mean().over(window1),
    "avg_min2": col("value").mean().over(window2),
    "avg_min3": col("value").mean().over(window3)
})

df.show()
```

In this example:
- For the first row (2023-01-01), the window frame contains only 2 rows (itself and 2023-01-02)
  - `avg_min1` and `avg_min2` will have values (as 1 and 2 <= number of rows in frame)
  - `avg_min3` will be NULL (as 3 > number of rows in frame)
- For the middle rows, all calculations will have values as all frames contain 3 rows
- For the last row (2023-01-05), similar to the first row, `avg_min3` will be NULL

This parameter is useful to ensure that calculations are only performed when enough data points are available for a meaningful result, particularly at the edges of your dataset or when using sliding windows.

## Applying Window Functions

Window functions are applied using the `.over()` method on expressions:

```python
df.with_column("result", some_function().over(window_spec))
```

### Supported Window Functions

#### Aggregate Functions

All aggregate functions in Daft can be used as window functions when applied with `.over()`:

- `col("value").sum().over(window_spec)`: Sum of values in the window
- `col("value").mean().over(window_spec)`: Average of values in the window
- `col("value").min().over(window_spec)`: Minimum value in the window
- `col("value").max().over(window_spec)`: Maximum value in the window
- `col("value").count().over(window_spec)`: Count of non-null values in the window

#### Ranking Functions

The following ranking functions are available:

- `row_number().over(window_spec)`: Returns the sequential row number starting from 1 within the partition
- `rank().over(window_spec)`: Returns the rank with gaps for ties
- `dense_rank().over(window_spec)`: Returns the rank without gaps for ties

#### Lead/Lag Functions

- `col("value").lead(n).over(window_spec)`: Accesses data from a subsequent row in the partition
- `col("value").lag(n).over(window_spec)`: Accesses data from a previous row in the partition

Where `n` is the number of rows to lead or lag (default is 1).

## Supported Window Configurations

Daft supports specific combinations of window features:

**Partition by only**:

- Supported functions: Aggregate functions only
- Example: `col("value").sum().over(Window().partition_by("category"))`

**Partition by + Order by**:

- Supported functions: Aggregate functions, Ranking functions, Lead/Lag functions
- Example: `row_number().over(Window().partition_by("category").order_by("date"))`

**Partition by + Order by + Rows between**:

- Supported functions: Aggregate functions only
- Example: `col("value").sum().over(Window().partition_by("category").order_by("date").rows_between(Window.unbounded_preceding, Window.current_row))`

## Unsupported Window Configurations

The following configurations are currently not supported:

1. **Order by only** (without partition by)
2. **Order by + Rows between** (without partition by)
3. **Range between** (in any configuration)

## Examples

### Basic Window Aggregation

Compute total sales per category:

```python
from daft import Window, col

window_spec = Window().partition_by("category")
df = df.with_column("category_total", col("sales").sum().over(window_spec))
```

### Running Totals

Compute a running sum of sales by date within each category:

```python
from daft import Window, col

running_window = Window().partition_by("category").order_by("date").rows_between(
    Window.unbounded_preceding, Window.current_row
)
df = df.with_column("running_total", col("sales").sum().over(running_window))
```

### Ranking

Rank products by sales within each category:

```python
from daft import Window, col, rank, dense_rank, row_number

window_spec = Window().partition_by("category").order_by("sales", desc=True)
df = df.with_columns({
    "sales_rank": rank().over(window_spec),
    "sales_dense_rank": dense_rank().over(window_spec),
    "sales_row_number": row_number().over(window_spec)
})
```

### Lead and Lag

Compare current date's sales with previous and next date's sales:

```python
from daft import Window, col

window_spec = Window().partition_by("category").order_by("date")
df = df.with_columns({
    "prev_day_sales": col("sales").lag(1).over(window_spec),
    "next_day_sales": col("sales").lead(1).over(window_spec),
    "day_over_day_change": col("sales") - col("sales").lag(1).over(window_spec)
})
```

### Moving Average

Compute a 7-day moving average of sales:

```python
from daft import Window, col

# Standard 7-day moving average
moving_avg_window = Window().partition_by("category").order_by("date").rows_between(-3, 3)
df = df.with_column("7day_moving_avg", col("sales").mean().over(moving_avg_window))

# 7-day moving average requiring at least 5 rows in the frame
robust_moving_avg_window = Window().partition_by("category").order_by("date").rows_between(-3, 3, min_periods=5)
df = df.with_column("robust_7day_avg", col("sales").mean().over(robust_moving_avg_window))
```

### Percentage of Total

Calculate each sale as a percentage of the category total:

```python
from daft import Window, col

window_spec = Window().partition_by("category")
df = df.with_column(
    "pct_of_category",
    (col("sales") * 100 / col("sales").sum().over(window_spec)).alias("pct_of_category")
)
```
