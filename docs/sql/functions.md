# SQL Functions

Daft SQL gives you access to Daft's expression functions directly in SQL. The
functions mirror the Python [functions API](../api/functions/index.md) and are
called with nesting instead of method chaining.

This page documents the SQL syntax and behavior of individual functions. For the
equivalent Python expressions, follow the links in each section.

## Date and Time Functions

### Extracting Date and Time Parts

`EXTRACT(<part> FROM <expr>)` returns a single component of a date or timestamp expression. Part names are case-insensitive.

The parts and their return types:

| Part | Returns | Type |
|------|---------|------|
| `YEAR` | Calendar year | `Int32` |
| `QUARTER` | Quarter of the year, 1–4 | `UInt32` |
| `MONTH` | Month of the year, 1–12 | `UInt32` |
| `DAY`, `DAY_OF_MONTH` | Day of the month, 1–31 | `UInt32` |
| `DAY_OF_WEEK` | Day of the week, Monday = 0 through Sunday = 6 | `UInt32` |
| `DAY_OF_YEAR` | Day of the year, 1–366 | `UInt32` |
| `WEEK_OF_YEAR` | [ISO week number](https://en.wikipedia.org/wiki/ISO_week_date), 1–53 | `UInt32` |
| `DATE` | The date portion of a timestamp | `Date` |
| `HOUR` | Hour, 0–23 | `UInt32` |
| `MINUTE` | Minute, 0–59 | `UInt32` |
| `SECOND` | Second, 0–59 | `UInt32` |
| `MILLISECOND` | Millisecond component, 0–999 | `UInt32` |
| `MICROSECOND` | Microsecond component, 0–999999 | `UInt32` |
| `NANOSECOND` | Nanosecond component, 0–999999999 | `UInt32` |
| `UNIX_DATE` | Days since the Unix epoch (timestamp inputs only) | `UInt64` |

`YEAR`, `QUARTER`, `MONTH`, `DAY`, `DAY_OF_WEEK`, `DAY_OF_YEAR`, `WEEK_OF_YEAR`, and `DATE` accept both dates and timestamps. `HOUR`, `MINUTE`, `SECOND`, `MILLISECOND`, `MICROSECOND`, and `NANOSECOND` accept timestamps or times, and `UNIX_DATE` requires a timestamp. Other parts, such as `EPOCH`, are not supported and raise an error.

=== "⚙️ SQL"
    ```python
    import datetime

    import daft

    df = daft.from_pydict(
        {
            "ts": [
                datetime.datetime(2025, 3, 4, 15, 30, 45, 123456),
                datetime.datetime(2024, 12, 31, 0, 0, 0),
            ]
        }
    )

    df = daft.sql(
        """
        SELECT
            EXTRACT(YEAR FROM ts) AS year,
            EXTRACT(QUARTER FROM ts) AS quarter,
            EXTRACT(DAY_OF_WEEK FROM ts) AS day_of_week,
            EXTRACT(WEEK_OF_YEAR FROM ts) AS week_of_year,
            EXTRACT(DATE FROM ts) AS date,
            EXTRACT(UNIX_DATE FROM ts) AS unix_date
        FROM df
        """
    )
    df.show()
    ```

=== "🐍 Python"
    ``` python
    import datetime

    import daft
    from daft import col

    df = daft.from_pydict(
        {
            "ts": [
                datetime.datetime(2025, 3, 4, 15, 30, 45, 123456),
                datetime.datetime(2024, 12, 31, 0, 0, 0),
            ]
        }
    )

    df = df.select(
        col("ts").year().alias("year"),
        col("ts").quarter().alias("quarter"),
        col("ts").day_of_week().alias("day_of_week"),
        col("ts").week_of_year().alias("week_of_year"),
        col("ts").date().alias("date"),
        col("ts").unix_date().alias("unix_date"),
    )
    df.show()
    ```

``` {title="Output"}

╭───────┬─────────┬─────────────┬──────────────┬────────────┬────────────╮
│ year  ┆ quarter ┆ day_of_week ┆ week_of_year ┆ date       ┆ unix_date  │
│ ---   ┆ ---     ┆ ---         ┆ ---          ┆ ---        ┆ ---        │
│ Int32 ┆ UInt32  ┆ UInt32      ┆ UInt32       ┆ Date       ┆ UInt64     │
╞═══════╪═════════╪═════════════╪══════════════╪════════════╪════════════╡
│ 2025  ┆ 1       ┆ 1           ┆ 10           ┆ 2025-03-04 ┆ 20151      │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2024  ┆ 4       ┆ 1           ┆ 1            ┆ 2024-12-31 ┆ 20088      │
╰───────┴─────────┴─────────────┴──────────────┴────────────┴────────────╯

(Showing first 2 of 2 rows)
```
