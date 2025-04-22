# Window Functions

Window functions in Daft SQL allow you to perform calculations across a set of rows that are related to the current row, similar to aggregate functions but without collapsing the result into a single row.

!!! warning "Warning"

    Window function support in Daft SQL is currently limited. Full SQL window function support is under development.

## Basic Syntax

The general syntax for window functions in Daft is:

```sql
function_name([expr]) OVER (
   [PARTITION BY expr_list]
   [ORDER BY order_list]
   [frame_clause]
)
```

Where:

- `function_name` is the name of the window function
- `PARTITION BY` divides the result set into partitions to which the window function is applied
- `ORDER BY` defines the logical order of rows within each partition
  - Note: NULL values are positioned at the end for ascending order (default) and at the beginning for descending order
- `frame_clause` defines a subset of rows in the current partition (window frame)

## Supported Window Functions

The following window functions are currently supported:

### Ranking Functions

- `ROW_NUMBER()`: Returns the sequential row number starting from 1 within the partition.

  ```sql
  SELECT
    category,
    value,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY value) as row_num
  FROM sales
  ```

### Aggregate Functions

All Daft aggregate functions can be used as window functions. Common examples include:

- `SUM([expr])`: Returns the sum of expression values.
- `AVG([expr])`: Returns the average of expression values.
- `COUNT([expr])`: Returns the count of non-null expression values.
- `MIN([expr])`: Returns the minimum expression value.
- `MAX([expr])`: Returns the maximum expression value.

Example:

```sql
SELECT
  category,
  value,
  SUM(value) OVER (PARTITION BY category) as category_total,
  AVG(value) OVER (PARTITION BY category) as category_avg
FROM sales
```

## Window Frame Specification

When using aggregate functions as window functions, you can specify a window frame to define which rows to include in the aggregation:

```sql
function_name([expr]) OVER (
   [PARTITION BY expr_list]
   [ORDER BY order_list]
   [ROWS | RANGE]
   BETWEEN frame_start AND frame_end
)
```

Where:

- `ROWS` indicates that the frame is defined by physical row count
- `RANGE` indicates that the frame is defined by logical value (not fully supported yet)
- `frame_start` and `frame_end` can be one of:
  - `UNBOUNDED PRECEDING`: All rows before the current row
  - `n PRECEDING`: n rows before the current row
  - `CURRENT ROW`: The current row
  - `n FOLLOWING`: n rows after the current row
  - `UNBOUNDED FOLLOWING`: All rows after the current row

Examples:

```sql
-- Running sum (includes all previous rows and current row)
SELECT
  category,
  value,
  SUM(value) OVER (
    PARTITION BY category
    ORDER BY value
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) as running_sum
FROM sales

-- Moving average of current row and 2 preceding rows
SELECT
  date,
  value,
  AVG(value) OVER (
    ORDER BY date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) as moving_avg
FROM time_series
```

## Using Window Functions with Python API

Daft also provides a Python API for window functions using the `Window` class:

```python
from daft import Window, col, row_number

# Create window specifications
window_spec = Window().partition_by("category").order_by("value")

# Running sum window (includes all previous rows and current row)
running_window = (
    Window()
    .partition_by("category")
    .order_by("value")
    .rows_between(Window.unbounded_preceding, Window.current_row)
)

# Apply window functions
result_df = (
    df
    .with_column("row_num", row_number().over(window_spec))
    .with_column("running_sum", col("value").sum().over(running_window))
)
```

## Limitations

1. `RANGE` frame type is not fully supported yet
2. Named window specifications (`WINDOW` clause) are not supported
3. `IGNORE NULLS` and `RESPECT NULLS` options are not supported
4. Some advanced window functions like `FIRST_VALUE`, `LAST_VALUE`, `LEAD`, and `LAG` are under development
