# Daft SQL

# SQL Reference

Welcome to Daft SQL Reference.

Daft's [SQL](https://en.wikipedia.org/wiki/SQL) dialect closely follows both DuckDB and PostgreSQL.



## Example

Please see [Sessions and Catalogs](../configuration/sessions-usage.md) for a detailed look at connecting data sources to Daft SQL.

```python
import daft

from daft import Session

# create a session
sess = Session()

# create temp tables
sess.create_temp_table("T", daft.from_pydict({ "a": [ 0, 1 ] }))
sess.create_temp_table("S", daft.from_pydict({ "b": [ 1, 0 ] }))

# execute sql
sess.sql("SELECT * FROM T, S").show()
"""
╭───────┬───────╮
│ a     ┆ b     │
│ ---   ┆ ---   │
│ Int64 ┆ Int64 │
╞═══════╪═══════╡
│ 0     ┆ 1     │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 1     ┆ 1     │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 0     ┆ 0     │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 1     ┆ 0     │
╰───────┴───────╯
"""
```

## Usage

### SQL with DataFrames

Daft's [`daft.sql`][daft.sql.sql.sql] function automatically detects any [`daft.DataFrame`][daft.DataFrame] objects in your current Python environment to let you query them easily by name.

=== "⚙️ SQL"
    ```python
    # Note the variable name `my_special_df`
    my_special_df = daft.from_pydict({"A": [1, 2, 3], "B": [1, 2, 3]})

    # Use the SQL table name "my_special_df" to refer to the above DataFrame!
    sql_df = daft.sql("SELECT A, B FROM my_special_df")

    sql_df.show()
    ```

``` {title="Output"}

╭───────┬───────╮
│ A     ┆ B     │
│ ---   ┆ ---   │
│ Int64 ┆ Int64 │
╞═══════╪═══════╡
│ 1     ┆ 1     │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 2     ┆ 2     │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 3     ┆ 3     │
╰───────┴───────╯

(Showing first 3 of 3 rows)
```

In the above example, we query the DataFrame called `"my_special_df"` by simply referring to it in the SQL command. This produces a new DataFrame `sql_df` which can natively integrate with the rest of your Daft query. You can also use table functions to query sources directly.

=== "🐍 Python"
    ```python
    daft.sql("SELECT * FROM read_parquet('s3://...')")
    daft.sql("SELECT * FROM read_iceberg('s3://.../metadata.json')")
    ```

### Table Function Options

Table functions take the path as the first positional argument, plus any of the options
below. Named arguments use either `=>` or `:=`:

=== "🐍 Python"
    ```python
    daft.sql("SELECT * FROM read_csv('data.csv', has_headers => false)")
    daft.sql("SELECT * FROM read_csv('data.csv', has_headers := false)")
    ```

`read_parquet`, `read_csv`, `read_json`, and `read_deltalake` also accept the path itself
as a named `path` argument:

=== "🐍 Python"
    ```python
    daft.sql("SELECT * FROM read_csv(path => 'data.csv')")
    ```

A list of paths is written as a SQL array:

=== "🐍 Python"
    ```python
    daft.sql("SELECT * FROM read_parquet(['a.parquet', 'b.parquet'])")
    ```

| Function | Options |
|---|---|
| `read_parquet` | `infer_schema`, `schema`, `coerce_int96_timestamp_unit`, `chunk_size`, `multithreaded`, `io_config`, `file_path_column`, `hive_partitioning`, `ignore_corrupt_files` |
| `read_csv` | `infer_schema`, `schema`, `has_headers`, `delimiter`, `double_quote`, `quote`, `escape_char`, `comment`, `allow_variable_columns`, `io_config`, `file_path_column`, `hive_partitioning`, `buffer_size`, `chunk_size`, `ignore_corrupt_files` |
| `read_json` | `infer_schema`, `schema`, `io_config`, `file_path_column`, `hive_partitioning`, `buffer_size`, `chunk_size`, `skip_empty_files` |
| `read_iceberg` | `snapshot_id`, `branch`, `tag`, `io_config`, `ignore_corrupt_files` |
| `read_deltalake` | `io_config` |

These mirror the corresponding Python reader arguments — see
[`read_parquet`][daft.read_parquet], [`read_csv`][daft.read_csv],
[`read_json`][daft.read_json], [`read_iceberg`][daft.read_iceberg], and
[`read_deltalake`][daft.read_deltalake] for what each one does, and
[Generic File Source Options](../connectors/generic-file-source-options.md)
for `ignore_corrupt_files` in depth.

A `schema` is given as a struct literal mapping column names to type names:

=== "🐍 Python"
    ```python
    daft.sql("""
        SELECT * FROM read_csv('data.csv', schema := {'a': 'int64', 'b': 'string'})
    """)
    ```

For Iceberg, `snapshot_id`, `branch`, and `tag` select which version to read and are
mutually exclusive:

=== "🐍 Python"
    ```python
    daft.sql("SELECT * FROM read_iceberg('/warehouse/db/t/metadata/v3.metadata.json', branch => 'audit')")
    ```

`ignore_corrupt_files` skips unreadable files instead of failing the query, and
`df.skipped_corrupt_files` reports what was skipped once the DataFrame is materialized:

=== "🐍 Python"
    ```python
    df = daft.sql("SELECT * FROM read_csv('s3://my-bucket/data/**/*.csv', ignore_corrupt_files => true)")
    df.collect()
    print(df.skipped_corrupt_files)
    ```

### SQL Expressions

SQL has the concept of expressions as well. Here is an example of a simple addition expression, adding columns `A` and `B` in SQL to produce a new column `C`.

We also present here the equivalent query for SQL and DataFrame. Notice how similar the concepts are!

=== "⚙️ SQL"
    ```python
    df = daft.from_pydict({"A": [1, 2, 3], "B": [1, 2, 3]})
    df = daft.sql("SELECT A + B as C FROM df")
    df.show()
    ```

=== "🐍 Python"
    ``` python
    expr = (daft.col("A") + daft.col("B")).alias("C")

    df = daft.from_pydict({"A": [1, 2, 3], "B": [1, 2, 3]})
    df = df.select(expr)
    df.show()
    ```

``` {title="Output"}

╭───────╮
│ C     │
│ ---   │
│ Int64 │
╞═══════╡
│ 2     │
├╌╌╌╌╌╌╌┤
│ 4     │
├╌╌╌╌╌╌╌┤
│ 6     │
╰───────╯

(Showing first 3 of 3 rows)
```

In the above query, both the SQL version of the query and the DataFrame version of the query produce the same result.

Under the hood, they run the same Expression `col("A") + col("B")`!

One really cool trick you can do is to use the [`daft.sql_expr`][daft.sql.sql.sql_expr] function as a helper to easily create Expressions. The following are equivalent:

=== "⚙️ SQL"
    ```python
    sql_expr = daft.sql_expr("A + B as C")
    print("SQL expression:", sql_expr)
    ```

=== "🐍 Python"
    ``` python
    py_expr = (daft.col("A") + daft.col("B")).alias("C")
    print("Python expression:", py_expr)
    ```

``` {title="Output"}

SQL expression: col(A) + col(B) as C
Python expression: col(A) + col(B) as C
```

This means that you can pretty much use SQL anywhere you use Python expressions, making Daft extremely versatile at mixing workflows which leverage both SQL and Python.

As an example, consider the filter query below and compare the two equivalent Python and SQL queries:

=== "⚙️ SQL"
    ```python
    df = daft.from_pydict({"A": [1, 2, 3], "B": [1, 2, 3]})

    # Daft automatically converts this string using `daft.sql_expr`
    df = df.where("A < 2")

    df.show()
    ```

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({"A": [1, 2, 3], "B": [1, 2, 3]})

    # Using Daft's Python Expression API
    df = df.where(df["A"] < 2)

    df.show()
    ```

``` {title="Output"}

╭───────┬───────╮
│ A     ┆ B     │
│ ---   ┆ ---   │
│ Int64 ┆ Int64 │
╞═══════╪═══════╡
│ 1     ┆ 1     │
╰───────┴───────╯

(Showing first 1 of 1 rows)
```

Pretty sweet! Of course, this support for running Expressions on your columns extends well beyond arithmetic as we'll see in the next section on SQL Functions.

### SQL Functions

SQL also has access to all of Daft's powerful [`daft.Expression`][daft.Expression] functionality through SQL functions.

However, unlike the Python Expression API which encourages method-chaining (e.g. `col("a").download().decode_image()`), in SQL you have to do function nesting instead (e.g. `"image_decode(url_download(a))"`).

Here is an example of an equivalent function call in SQL vs Python:

=== "⚙️ SQL"
    ```python
    df = daft.from_pydict({"urls": [
        "https://user-images.githubusercontent.com/17691182/190476440-28f29e87-8e3b-41c4-9c28-e112e595f558.png",
        "https://user-images.githubusercontent.com/17691182/190476440-28f29e87-8e3b-41c4-9c28-e112e595f558.png",
        "https://user-images.githubusercontent.com/17691182/190476440-28f29e87-8e3b-41c4-9c28-e112e595f558.png",
    ]})
    df = daft.sql("SELECT image_decode(url_download(urls)) FROM df")
    df.show()
    ```

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({"urls": [
        "https://user-images.githubusercontent.com/17691182/190476440-28f29e87-8e3b-41c4-9c28-e112e595f558.png",
        "https://user-images.githubusercontent.com/17691182/190476440-28f29e87-8e3b-41c4-9c28-e112e595f558.png",
        "https://user-images.githubusercontent.com/17691182/190476440-28f29e87-8e3b-41c4-9c28-e112e595f558.png",
    ]})
    df = df.select(daft.col("urls").download().decode_image())
    df.show()
    ```

``` {title="Output"}

╭──────────────╮
│ urls         │
│ ---          │
│ Image[MIXED] │
╞══════════════╡
│ <Image>      │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ <Image>      │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ <Image>      │
╰──────────────╯

(Showing first 3 of 3 rows)
```

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
