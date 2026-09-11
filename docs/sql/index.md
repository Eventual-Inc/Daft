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
