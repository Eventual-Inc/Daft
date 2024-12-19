# DataFrame

!!! failure "todo(docs): Check that this page makes sense. Can we have a 1-1 mapping of "Common data operations that you would perform on DataFrames are: ..." to its respective section?"

!!! failure "todo(docs): I reused some of these sections in the Quickstart (create df, execute df and view data, select rows, select columns) but the examples in the quickstart are different. Should we still keep those sections on this page?"


If you are coming from other DataFrame libraries such as Pandas or Polars, here are some key differences about Daft DataFrames:

1. **Distributed:** When running in a distributed cluster, Daft splits your data into smaller "chunks" called *Partitions*. This allows Daft to process your data in parallel across multiple machines, leveraging more resources to work with large datasets.

2. **Lazy:** When you write operations on a DataFrame, Daft doesn't execute them immediately. Instead, it creates a plan (called a query plan) of what needs to be done. This plan is optimized and only executed when you specifically request the results, which can lead to more efficient computations.

3. **Multimodal:** Unlike traditional tables that usually contain simple data types like numbers and text, Daft DataFrames can handle complex data types in its columns. This includes things like images, audio files, or even custom Python objects.

For a full comparison between Daft and other DataFrame Libraries, see [DataFrame Comparison](../resources/dataframe_comparison.md).

Common data operations that you would perform on DataFrames are:

1. [**Filtering rows:**](dataframe.md/#selecting-rows) Use [`df.where(...)`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.where.html#daft.DataFrame.where) to keep only the rows that meet certain conditions.
2. **Creating new columns:** Use [`df.with_column(...)`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.with_column.html#daft.DataFrame.with_column) to add a new column based on calculations from existing ones.
3. [**Joining DataFrames:**](dataframe.md/#combining-dataframes) Use [`df.join(other_df, ...)`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.join.html#daft.DataFrame.join) to combine two DataFrames based on common columns.
4. [**Sorting:**](dataframe.md#reordering-rows) Use [`df.sort(...)`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.sort.html#daft.DataFrame.sort) to arrange your data based on values in one or more columns.
5. **Grouping and aggregating:** Use [`df.groupby(...).agg(...)`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.groupby.html#daft.DataFrame.groupby) to summarize your data by groups.

## Creating a DataFrame

!!! tip "See Also"

    [Reading/Writing Data](read_write.md) - a more in-depth guide on various options for reading/writing data to/from Daft DataFrames from in-memory data (Python, Arrow), files (Parquet, CSV, JSON), SQL Databases and Data Catalogs

Let's create our first Dataframe from a Python dictionary of columns.

=== "🐍 Python"

    ```python
    import daft

    df = daft.from_pydict({
        "A": [1, 2, 3, 4],
        "B": [1.5, 2.5, 3.5, 4.5],
        "C": [True, True, False, False],
        "D": [None, None, None, None],
    })
    ```

Examine your Dataframe by printing it:

```
df
```

``` {title="Output"}

╭───────┬─────────┬─────────┬──────╮
│ A     ┆ B       ┆ C       ┆ D    │
│ ---   ┆ ---     ┆ ---     ┆ ---  │
│ Int64 ┆ Float64 ┆ Boolean ┆ Null │
╞═══════╪═════════╪═════════╪══════╡
│ 1     ┆ 1.5     ┆ true    ┆ None │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ 2     ┆ 2.5     ┆ true    ┆ None │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ 3     ┆ 3.5     ┆ false   ┆ None │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ 4     ┆ 4.5     ┆ false   ┆ None │
╰───────┴─────────┴─────────┴──────╯

(Showing first 4 of 4 rows)
```

Congratulations - you just created your first DataFrame! It has 4 columns, "A", "B", "C", and "D". Let's try to select only the "A", "B", and "C" columns:

=== "🐍 Python"
    ``` python
    df = df.select("A", "B", "C")
    df
    ```

=== "⚙️ SQL"
    ```python
    df = daft.sql("SELECT A, B, C FROM df")
    df
    ```

``` {title="Output"}

╭───────┬─────────┬─────────╮
│ A     ┆ B       ┆ C       │
│ ---   ┆ ---     ┆ ---     │
│ Int64 ┆ Float64 ┆ Boolean │
╰───────┴─────────┴─────────╯

(No data to display: Dataframe not materialized)
```

But wait - why is it printing the message `(No data to display: Dataframe not materialized)` and where are the rows of each column?

## Executing DataFrame and Viewing Data

The reason that our DataFrame currently does not display its rows is that Daft DataFrames are **lazy**. This just means that Daft DataFrames will defer all its work until you tell it to execute.

In this case, Daft is just deferring the work required to read the data and select columns, however in practice this laziness can be very useful for helping Daft optimize your queries before execution!

!!! info "Info"

    When you call methods on a Daft Dataframe, it defers the work by adding to an internal "plan". You can examine the current plan of a DataFrame by calling [`df.explain()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.explain.html#daft.DataFrame.explain)!

    Passing the `show_all=True` argument will show you the plan after Daft applies its query optimizations and the physical (lower-level) plan.

    ```
    Plan Output

    == Unoptimized Logical Plan ==

    * Project: col(A), col(B), col(C)
    |
    * Source:
    |   Number of partitions = 1
    |   Output schema = A#Int64, B#Float64, C#Boolean, D#Null


    == Optimized Logical Plan ==

    * Project: col(A), col(B), col(C)
    |
    * Source:
    |   Number of partitions = 1
    |   Output schema = A#Int64, B#Float64, C#Boolean, D#Null


    == Physical Plan ==

    * Project: col(A), col(B), col(C)
    |   Clustering spec = { Num partitions = 1 }
    |
    * InMemoryScan:
    |   Schema = A#Int64, B#Float64, C#Boolean, D#Null,
    |   Size bytes = 65,
    |   Clustering spec = { Num partitions = 1 }
    ```

We can tell Daft to execute our DataFrame and store the results in-memory using [`df.collect()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.collect.html#daft.DataFrame.collect):

=== "🐍 Python"
    ``` python
    df.collect()
    df
    ```

``` {title="Output"}
╭───────┬─────────┬─────────┬──────╮
│ A     ┆ B       ┆ C       ┆ D    │
│ ---   ┆ ---     ┆ ---     ┆ ---  │
│ Int64 ┆ Float64 ┆ Boolean ┆ Null │
╞═══════╪═════════╪═════════╪══════╡
│ 1     ┆ 1.5     ┆ true    ┆ None │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ 2     ┆ 2.5     ┆ true    ┆ None │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ 3     ┆ 3.5     ┆ false   ┆ None │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ 4     ┆ 4.5     ┆ false   ┆ None │
╰───────┴─────────┴─────────┴──────╯

(Showing first 4 of 4 rows)
```

Now your DataFrame object `df` is **materialized** - Daft has executed all the steps required to compute the results, and has cached the results in memory so that it can display this preview.

Any subsequent operations on `df` will avoid recomputations, and just use this materialized result!

### When should I materialize my DataFrame?

If you "eagerly" call [`df.collect()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.collect.html#daft.DataFrame.collect) immediately on every DataFrame, you may run into issues:

1. If data is too large at any step, materializing all of it may cause memory issues
2. Optimizations are not possible since we cannot "predict future operations"

However, data science is all about experimentation and trying different things on the same data. This means that materialization is crucial when working interactively with DataFrames, since it speeds up all subsequent experimentation on that DataFrame.

We suggest materializing DataFrames using [`df.collect()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.collect.html#daft.DataFrame.collect) when they contain expensive operations (e.g. sorts or expensive function calls) and have to be called multiple times by downstream code:

=== "🐍 Python"
    ``` python
    df = df.sort("A")  # expensive sort
    df.collect()  # materialize the DataFrame

    # All subsequent work on df avoids recomputing previous steps
    df.sum("B").show()
    df.mean("B").show()
    df.with_column("try_this", df["A"] + 1).show(5)
    ```

=== "⚙️ SQL"
    ```python
    df = daft.sql("SELECT * FROM df ORDER BY A")
    df.collect()

    # All subsequent work on df avoids recomputing previous steps
    daft.sql("SELECT sum(B) FROM df").show()
    daft.sql("SELECT mean(B) FROM df").show()
    daft.sql("SELECT *, (A + 1) AS try_this FROM df").show(5)
    ```

``` {title="Output"}

╭─────────╮
│ B       │
│ ---     │
│ Float64 │
╞═════════╡
│ 12      │
╰─────────╯

(Showing first 1 of 1 rows)

╭─────────╮
│ B       │
│ ---     │
│ Float64 │
╞═════════╡
│ 3       │
╰─────────╯

(Showing first 1 of 1 rows)

╭───────┬─────────┬─────────┬──────────╮
│ A     ┆ B       ┆ C       ┆ try_this │
│ ---   ┆ ---     ┆ ---     ┆ ---      │
│ Int64 ┆ Float64 ┆ Boolean ┆ Int64    │
╞═══════╪═════════╪═════════╪══════════╡
│ 1     ┆ 1.5     ┆ true    ┆ 2        │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
│ 2     ┆ 2.5     ┆ true    ┆ 3        │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
│ 3     ┆ 3.5     ┆ false   ┆ 4        │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
│ 4     ┆ 4.5     ┆ false   ┆ 5        │
╰───────┴─────────┴─────────┴──────────╯

(Showing first 4 of 4 rows)
```

In many other cases however, there are better options than materializing your entire DataFrame with [`df.collect()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.collect.html#daft.DataFrame.collect):

1. **Peeking with df.show(N)**: If you only want to "peek" at the first few rows of your data for visualization purposes, you can use [`df.show(N)`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.show.html#daft.DataFrame.show), which processes and shows only the first `N` rows.
2. **Writing to disk**: The `df.write_*` methods will process and write your data to disk per-partition, avoiding materializing it all in memory at once.
3. **Pruning data**: You can materialize your DataFrame after performing a [`df.limit()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.limit.html#daft.DataFrame.limit), [`df.where()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.where.html#daft.DataFrame.where) or [`df.select()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.select.html#daft.DataFrame.select) operation which processes your data or prune it down to a smaller size.

## Schemas and Types

Notice also that when we printed our DataFrame, Daft displayed its **schema**. Each column of your DataFrame has a **name** and a **type**, and all data in that column will adhere to that type!

Daft can display your DataFrame's schema without materializing it. Under the hood, it performs intelligent sampling of your data to determine the appropriate schema, and if you make any modifications to your DataFrame it can infer the resulting types based on the operation.

!!! note "Note"

    Under the hood, Daft represents data in the [Apache Arrow](https://arrow.apache.org/) format, which allows it to efficiently represent and work on data using high-performance kernels which are written in Rust.

## Running Computation with Expressions

To run computations on data in our DataFrame, we use Expressions.

The following statement will [`df.show()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.show.html#daft.DataFrame.show) a DataFrame that has only one column - the column `A` from our original DataFrame but with every row incremented by 1.

=== "🐍 Python"
    ``` python
    df.select(df["A"] + 1).show()
    ```

=== "⚙️ SQL"
    ```python
    daft.sql("SELECT A + 1 FROM df").show()
    ```

``` {title="Output"}

╭───────╮
│ A     │
│ ---   │
│ Int64 │
╞═══════╡
│ 2     │
├╌╌╌╌╌╌╌┤
│ 3     │
├╌╌╌╌╌╌╌┤
│ 4     │
├╌╌╌╌╌╌╌┤
│ 5     │
╰───────╯

(Showing first 4 of 4 rows)
```

!!! info "Info"

    A common pattern is to create a new columns using [`DataFrame.with_column`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.with_column.html):

    === "🐍 Python"
        ``` python
        # Creates a new column named "foo" which takes on values
        # of column "A" incremented by 1
        df = df.with_column("foo", df["A"] + 1)
        df.show()
        ```

    === "⚙️ SQL"
        ```python
        # Creates a new column named "foo" which takes on values
        # of column "A" incremented by 1
        df = daft.sql("SELECT *, A + 1 AS foo FROM df")
        df.show()
        ```

``` {title="Output"}

╭───────┬─────────┬─────────┬───────╮
│ A     ┆ B       ┆ C       ┆ foo   │
│ ---   ┆ ---     ┆ ---     ┆ ---   │
│ Int64 ┆ Float64 ┆ Boolean ┆ Int64 │
╞═══════╪═════════╪═════════╪═══════╡
│ 1     ┆ 1.5     ┆ true    ┆ 2     │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 2     ┆ 2.5     ┆ true    ┆ 3     │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 3     ┆ 3.5     ┆ false   ┆ 4     │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 4     ┆ 4.5     ┆ false   ┆ 5     │
╰───────┴─────────┴─────────┴───────╯

(Showing first 4 of 4 rows)
```

Congratulations, you have just written your first **Expression**: `df["A"] + 1`! Expressions are a powerful way of describing computation on columns. For more details, check out the next section on [Expressions](expressions.md).

<!-- In a previous section, we covered Expressions which are ways of expressing computation on a single column.

However, the Daft DataFrame is a table containing equal-length columns. Many operations affect the entire table at once, which in turn affects the ordering or sizes of all columns.

This section of the user guide covers these operations, and how to use them. -->

## Selecting Rows

We can limit the rows to the first ``N`` rows using [`df.limit(N)`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.limit.html#daft.DataFrame.limit):

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({
        "A": [1, 2, 3, 4, 5],
        "B": [6, 7, 8, 9, 10],
    })

    df.limit(3).show()
    ```

``` {title="Output"}

+---------+---------+
|       A |       B |
|   Int64 |   Int64 |
+=========+=========+
|       1 |       6 |
+---------+---------+
|       2 |       7 |
+---------+---------+
|       3 |       8 |
+---------+---------+
(Showing first 3 rows)
```

We can also filter rows using [`df.where()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.where.html#daft.DataFrame.where), which takes an input a Logical Expression predicate:

=== "🐍 Python"
    ``` python
    df.where(df["A"] > 3).show()
    ```

``` {title="Output"}

+---------+---------+
|       A |       B |
|   Int64 |   Int64 |
+=========+=========+
|       4 |       9 |
+---------+---------+
|       5 |      10 |
+---------+---------+
(Showing first 2 rows)
```

## Selecting Columns

Select specific columns in a DataFrame using [`df.select()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.select.html#daft.DataFrame.select), which also takes Expressions as an input.

=== "🐍 Python"
    ``` python
    import daft

    df = daft.from_pydict({"A": [1, 2, 3], "B": [4, 5, 6]})

    df.select("A").show()
    ```

``` {title="Output"}

+---------+
|       A |
|   Int64 |
+=========+
|       1 |
+---------+
|       2 |
+---------+
|       3 |
+---------+
(Showing first 3 rows)
```

A useful alias for [`df.select()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.select.html#daft.DataFrame.select) is indexing a DataFrame with a list of column names or Expressions:

=== "🐍 Python"
    ``` python
    df[["A", "B"]].show()
    ```

``` {title="Output"}

+---------+---------+
|       A |       B |
|   Int64 |   Int64 |
+=========+=========+
|       1 |       4 |
+---------+---------+
|       2 |       5 |
+---------+---------+
|       3 |       6 |
+---------+---------+
(Showing first 3 rows)
```

Sometimes, it may be useful to exclude certain columns from a DataFrame. This can be done with [`df.exclude()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.exclude.html#daft.DataFrame.exclude):

=== "🐍 Python"
    ``` python
    df.exclude("A").show()
    ```

```{title="Output"}

+---------+
|       B |
|   Int64 |
+=========+
|       4 |
+---------+
|       5 |
+---------+
|       6 |
+---------+
(Showing first 3 rows)
```

Adding a new column can be achieved with [`df.with_column()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.with_column.html#daft.DataFrame.with_column):

=== "🐍 Python"
    ``` python
    df.with_column("C", df["A"] + df["B"]).show()
    ```

``` {title="Output"}

+---------+---------+---------+
|       A |       B |       C |
|   Int64 |   Int64 |   Int64 |
+=========+=========+=========+
|       1 |       4 |       5 |
+---------+---------+---------+
|       2 |       5 |       7 |
+---------+---------+---------+
|       3 |       6 |       9 |
+---------+---------+---------+
(Showing first 3 rows)
```

### Selecting Columns Using Wildcards

We can select multiple columns at once using wildcards. The expression [`.col(*)`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/expression_methods/daft.col.html#daft.col) selects every column in a DataFrame, and you can operate on this expression in the same way as a single column:

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({"A": [1, 2, 3], "B": [4, 5, 6]})
    df.select(col("*") * 3).show()
    ```

``` {title="Output"}
╭───────┬───────╮
│ A     ┆ B     │
│ ---   ┆ ---   │
│ Int64 ┆ Int64 │
╞═══════╪═══════╡
│ 3     ┆ 12    │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 6     ┆ 15    │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 9     ┆ 18    │
╰───────┴───────╯
```

We can also select multiple columns within structs using `col("struct.*")`:

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({
        "A": [
            {"B": 1, "C": 2},
            {"B": 3, "C": 4}
        ]
    })
    df.select(col("A.*")).show()
    ```

``` {title="Output"}

╭───────┬───────╮
│ B     ┆ C     │
│ ---   ┆ ---   │
│ Int64 ┆ Int64 │
╞═══════╪═══════╡
│ 1     ┆ 2     │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 3     ┆ 4     │
╰───────┴───────╯
```

Under the hood, wildcards work by finding all of the columns that match, then copying the expression several times and replacing the wildcard. This means that there are some caveats:

* Only one wildcard is allowed per expression tree. This means that `col("*") + col("*")` and similar expressions do not work.
* Be conscious about duplicated column names. Any code like `df.select(col("*"), col("*") + 3)` will not work because the wildcards expand into the same column names.

  For the same reason, `col("A") + col("*")` will not work because the name on the left-hand side is inherited, meaning all the output columns are named `A`, causing an error if there is more than one.
  However, `col("*") + col("A")` will work fine.

## Combining DataFrames

Two DataFrames can be column-wise joined using [`df.join()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.join.html#daft.DataFrame.join).

This requires a "join key", which can be supplied as the `on` argument if both DataFrames have the same name for their key columns, or the `left_on` and `right_on` argument if the key column has different names in each DataFrame.

Daft also supports multi-column joins if you have a join key comprising of multiple columns!

=== "🐍 Python"
    ``` python
    df1 = daft.from_pydict({"A": [1, 2, 3], "B": [4, 5, 6]})
    df2 = daft.from_pydict({"A": [1, 2, 3], "C": [7, 8, 9]})

    df1.join(df2, on="A").show()
    ```

``` {title="Output"}

+---------+---------+---------+
|       A |       B |       C |
|   Int64 |   Int64 |   Int64 |
+=========+=========+=========+
|       1 |       4 |       7 |
+---------+---------+---------+
|       2 |       5 |       8 |
+---------+---------+---------+
|       3 |       6 |       9 |
+---------+---------+---------+
(Showing first 3 rows)
```

## Reordering Rows

Rows in a DataFrame can be reordered based on some column using [`df.sort()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.sort.html#daft.DataFrame.sort). Daft also supports multi-column sorts for sorting on multiple columns at once.

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({
        "A": [1, 2, 3],
        "B": [6, 7, 8],
    })

    df.sort("A", desc=True).show()
    ```

```{title="Output"}

+---------+---------+
|       A |       B |
|   Int64 |   Int64 |
+=========+=========+
|       3 |       8 |
+---------+---------+
|       2 |       7 |
+---------+---------+
|       1 |       6 |
+---------+---------+
(Showing first 3 rows)
```

## Exploding Columns

The [`df.explode()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.explode.html#daft.DataFrame.explode) method can be used to explode a column containing a list of values into multiple rows. All other rows will be **duplicated**.

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({
        "A": [1, 2, 3],
        "B": [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
    })

    df.explode("B").show()
    ```

``` {title="Output"}

+---------+---------+
|       A |       B |
|   Int64 |   Int64 |
+=========+=========+
|       1 |       1 |
+---------+---------+
|       1 |       2 |
+---------+---------+
|       1 |       3 |
+---------+---------+
|       2 |       4 |
+---------+---------+
|       2 |       5 |
+---------+---------+
|       2 |       6 |
+---------+---------+
|       3 |       7 |
+---------+---------+
|       3 |       8 |
+---------+---------+
(Showing first 8 rows)
```

<!-- Commented out because there's Advanced/Partitioning section -->
<!-- ## Repartitioning

Daft is a distributed DataFrame, and the dataframe is broken into multiple "partitions" which are processed in parallel across the cores in your machine or cluster.

You may choose to increase or decrease the number of partitions with [`df.repartition()`](https://www.getdaft.io/projects/docs/en/stable/api_docs/doc_gen/dataframe_methods/daft.DataFrame.repartition.html#daft.DataFrame.repartition).

1. Increasing the number of partitions to 2x the total number of CPUs could help with resource utilization
2. If each partition is potentially overly large (e.g. containing large images), causing memory issues, you may increase the number of partitions to reduce the size of each individual partition
3. If you have too many partitions, global operations such as a sort or a join may take longer to execute

A good rule of thumb is to keep the number of partitions as twice the number of CPUs available on your backend, increasing the number of partitions as necessary if they cannot be processed in memory. -->
