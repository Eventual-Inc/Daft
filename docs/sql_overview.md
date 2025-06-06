# Daft SQL

Daft's [SQL](https://en.wikipedia.org/wiki/SQL) dialect closely follows both DuckDB and PostgreSQL. For a full list of SQL operations, check out our [SQL Reference](sql/index.md).

## Example

Please see [Sessions](sessions.md) and [Catalogs](catalogs/index.md) for a detailed look at connecting data sources to Daft SQL.

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

However, unlike the Python Expression API which encourages method-chaining (e.g. `col("a").url.download().image.decode()`), in SQL you have to do function nesting instead (e.g. `"image_decode(url_download(a))"`).

!!! note "Note"

    A full catalog of the available SQL Functions in Daft is available in [`SQL Reference`](sql/index.md).

    Note that it closely mirrors the Python API, with some function naming differences vs the available Python methods. We also have some aliased functions for ANSI SQL-compliance or familiarity to users coming from other common SQL dialects such as PostgreSQL and SparkSQL to easily find their functionality.

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
    df = df.select(daft.col("urls").url.download().image.decode())
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
