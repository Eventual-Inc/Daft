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
    - Note: By default, NULL values are positioned at the end for ascending order (default) and at the beginning for descending order. To override this behavior you can specify "NULLS FIRST" or "NULLS LAST".
- `frame_clause` defines a subset of rows in the current partition (called the window frame)

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

- `RANK()`: Returns the rank of the current row within a partition, with gaps in the ranking sequence when there are ties.

    ```sql
    SELECT
        category,
        value,
        RANK() OVER (PARTITION BY category ORDER BY value) as rank
    FROM sales
    ```

- `DENSE_RANK()`: Returns the rank of the current row within a partition, without gaps in the ranking sequence when there are ties.

    ```sql
    SELECT
        category,
        value,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY value) as dense_rank
    FROM sales
    ```

### Offset Functions

- `LAG(value [, offset [, default]])`: Returns the value from a row that is offset rows before the current row. If no such row exists, returns the default value. The offset parameter defaults to 1 if not specified.

    ```sql
    SELECT
        date,
        value,
        LAG(value, 1, 0) OVER (ORDER BY date) as previous_value
    FROM time_series
    ```

- `LEAD(value [, offset [, default]])`: Returns the value from a row that is offset rows after the current row. If no such row exists, returns the default value. The offset parameter defaults to 1 if not specified.

    ```sql
    SELECT
        date,
        value,
        LEAD(value, 1, 0) OVER (ORDER BY date) as next_value
    FROM time_series
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

!!! note "Note"
    When using aggregate functions with both `PARTITION BY` and `ORDER BY`, the default window frame includes all rows from the start of the partition up to the current row — equivalent to `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.

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

    - `UNBOUNDED PRECEDING`: All rows before the current row (only valid for `frame_start`)
    - `n PRECEDING`: n rows before the current row
    - `CURRENT ROW`: The current row
    - `n FOLLOWING`: n rows after the current row
    - `UNBOUNDED FOLLOWING`: All rows after the current row (only valid for `frame_end`)

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

## Limitations

1. Global partitions (window functions without `PARTITION BY`) are not yet supported
2. Named window specifications (`WINDOW` clause) are not supported
3. `IGNORE NULLS` and `RESPECT NULLS` options are not supported
