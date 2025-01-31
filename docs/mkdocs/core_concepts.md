# Core Concepts

Learn about the core concepts that Daft is built on!

## DataFrame

If you are coming from other DataFrame libraries such as Pandas or Polars, here are some key differences about Daft DataFrames:

1. **Distributed:** When running in a distributed cluster, Daft splits your data into smaller "chunks" called *Partitions*. This allows Daft to process your data in parallel across multiple machines, leveraging more resources to work with large datasets.

2. **Lazy:** When you write operations on a DataFrame, Daft doesn't execute them immediately. Instead, it creates a plan (called a query plan) of what needs to be done. This plan is optimized and only executed when you specifically request the results, which can lead to more efficient computations.

3. **Multimodal:** Unlike traditional tables that usually contain simple data types like numbers and text, Daft DataFrames can handle complex data types in its columns. This includes things like images, audio files, or even custom Python objects.

For a full comparison between Daft and other DataFrame Libraries, see [DataFrame Comparison](resources/dataframe_comparison.md).

Common data operations that you would perform on DataFrames are:

1. [**Filtering rows:**](core_concepts.md#selecting-rows) Use [`df.where(...)`]({{ api_path }}/dataframe_methods/daft.DataFrame.where.html) to keep only the rows that meet certain conditions.
2. **Creating new columns:** Use [`df.with_column(...)`]({{ api_path }}/dataframe_methods/daft.DataFrame.with_column.html) to add a new column based on calculations from existing ones.
3. [**Joining DataFrames:**](core_concepts.md#combining-dataframes) Use [`df.join(other_df, ...)`]({{ api_path }}/dataframe_methods/daft.DataFrame.join.html) to combine two DataFrames based on common columns.
4. [**Sorting:**](core_concepts.md#reordering-rows) Use [`df.sort(...)`]({{ api_path }}/dataframe_methods/daft.DataFrame.sort.html) to arrange your data based on values in one or more columns.
5. [**Grouping and aggregating:**](core_concepts.md#aggregations-and-grouping) Use [`df.groupby(...).agg(...)`]({{ api_path }}/dataframe_methods/daft.DataFrame.groupby.html) to summarize your data by groups.

### Creating a Dataframe

!!! tip "See Also"

    [Reading Data](core_concepts.md#reading-data) and [Writing Data](core_concepts.md#writing-data) - a more in-depth guide on various options for reading and writing data to and from Daft DataFrames from in-memory data (Python, Arrow), files (Parquet, CSV, JSON), SQL Databases and Data Catalogs

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

### Executing DataFrame and Viewing Data

The reason that our DataFrame currently does not display its rows is that Daft DataFrames are **lazy**. This just means that Daft DataFrames will defer all its work until you tell it to execute.

In this case, Daft is just deferring the work required to read the data and select columns, however in practice this laziness can be very useful for helping Daft optimize your queries before execution!

!!! info "Info"

    When you call methods on a Daft Dataframe, it defers the work by adding to an internal "plan". You can examine the current plan of a DataFrame by calling [`df.explain()`]({{ api_path }}/dataframe_methods/daft.DataFrame.explain.html)!

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

We can tell Daft to execute our DataFrame and store the results in-memory using [`df.collect()`]({{ api_path }}/dataframe_methods/daft.DataFrame.collect.html):

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

If you "eagerly" call [`df.collect()`]({{ api_path }}/dataframe_methods/daft.DataFrame.collect.html) immediately on every DataFrame, you may run into issues:

1. If data is too large at any step, materializing all of it may cause memory issues
2. Optimizations are not possible since we cannot "predict future operations"

However, data science is all about experimentation and trying different things on the same data. This means that materialization is crucial when working interactively with DataFrames, since it speeds up all subsequent experimentation on that DataFrame.

We suggest materializing DataFrames using [`df.collect()`]({{ api_path }}/dataframe_methods/daft.DataFrame.collect.html) when they contain expensive operations (e.g. sorts or expensive function calls) and have to be called multiple times by downstream code:

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

In many other cases however, there are better options than materializing your entire DataFrame with [`df.collect()`]({{ api_path }}/dataframe_methods/daft.DataFrame.collect.html):

1. **Peeking with df.show(N)**: If you only want to "peek" at the first few rows of your data for visualization purposes, you can use [`df.show(N)`]({{ api_path }}/dataframe_methods/daft.DataFrame.show.html), which processes and shows only the first `N` rows.
2. **Writing to disk**: The `df.write_*` methods will process and write your data to disk per-partition, avoiding materializing it all in memory at once.
3. **Pruning data**: You can materialize your DataFrame after performing a [`df.limit()`]({{ api_path }}/dataframe_methods/daft.DataFrame.limit.html), [`df.where()`]({{ api_path }}/dataframe_methods/daft.DataFrame.where.html) or [`df.select()`]({{ api_path }}/dataframe_methods/daft.DataFrame.select.html) operation which processes your data or prune it down to a smaller size.

### Schemas and Types

Notice also that when we printed our DataFrame, Daft displayed its **schema**. Each column of your DataFrame has a **name** and a **type**, and all data in that column will adhere to that type!

Daft can display your DataFrame's schema without materializing it. Under the hood, it performs intelligent sampling of your data to determine the appropriate schema, and if you make any modifications to your DataFrame it can infer the resulting types based on the operation.

!!! note "Note"

    Under the hood, Daft represents data in the [Apache Arrow](https://arrow.apache.org/) format, which allows it to efficiently represent and work on data using high-performance kernels which are written in Rust.

### Running Computation with Expressions

To run computations on data in our DataFrame, we use Expressions.

The following statement will [`df.show()`]({{ api_path }}/dataframe_methods/daft.DataFrame.show.html) a DataFrame that has only one column - the column `A` from our original DataFrame but with every row incremented by 1.

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

    A common pattern is to create a new columns using [`DataFrame.with_column`]({{ api_path }}/dataframe_methods/daft.DataFrame.with_column.html):

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

Congratulations, you have just written your first **Expression**: `df["A"] + 1`! Expressions are a powerful way of describing computation on columns. For more details, check out the next section on [Expressions](core_concepts.md#expressions).

<!-- In a previous section, we covered Expressions which are ways of expressing computation on a single column.

However, the Daft DataFrame is a table containing equal-length columns. Many operations affect the entire table at once, which in turn affects the ordering or sizes of all columns.

This section of the user guide covers these operations, and how to use them. -->

### Selecting Rows

We can limit the rows to the first ``N`` rows using [`df.limit(N)`]({{ api_path }}/dataframe_methods/daft.DataFrame.limit.html):

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

We can also filter rows using [`df.where()`]({{ api_path }}/dataframe_methods/daft.DataFrame.where.html), which takes an input a Logical Expression predicate:

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

### Selecting Columns

Select specific columns in a DataFrame using [`df.select()`]({{ api_path }}/dataframe_methods/daft.DataFrame.select.html), which also takes Expressions as an input.

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

A useful alias for [`df.select()`]({{ api_path }}/dataframe_methods/daft.DataFrame.select.html) is indexing a DataFrame with a list of column names or Expressions:

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

Sometimes, it may be useful to exclude certain columns from a DataFrame. This can be done with [`df.exclude()`]({{ api_path }}/dataframe_methods/daft.DataFrame.exclude.html):

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

Adding a new column can be achieved with [`df.with_column()`]({{ api_path }}/dataframe_methods/daft.DataFrame.with_column.html):

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

#### Selecting Columns Using Wildcards

We can select multiple columns at once using wildcards. The expression [`.col(*)`]({{ api_path }}/expression_methods/daft.col.html) selects every column in a DataFrame, and you can operate on this expression in the same way as a single column:

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

### Combining DataFrames

Two DataFrames can be column-wise joined using [`df.join()`]({{ api_path }}/dataframe_methods/daft.DataFrame.join.html).

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

### Reordering Rows

Rows in a DataFrame can be reordered based on some column using [`df.sort()`]({{ api_path }}/dataframe_methods/daft.DataFrame.sort.html). Daft also supports multi-column sorts for sorting on multiple columns at once.

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

### Exploding Columns

The [`df.explode()`]({{ api_path }}/dataframe_methods/daft.DataFrame.explode.html) method can be used to explode a column containing a list of values into multiple rows. All other rows will be **duplicated**.

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

## Expressions

Expressions are how you can express computations that should be run over columns of data.

### Creating Expressions

#### Referring to a column in a DataFrame

Most commonly you will be creating expressions by using the [`daft.col`]({{ api_path }}/expression_methods/daft.col.html) function.

=== "🐍 Python"
    ``` python
    # Refers to column "A"
    daft.col("A")
    ```

=== "⚙️ SQL"
    ```python
    daft.sql_expr("A")
    ```

``` {title="Output"}

col(A)
```

The above code creates an Expression that refers to a column named `"A"`.

### Using SQL

Daft can also parse valid SQL as expressions.

=== "⚙️ SQL"
    ```python
    daft.sql_expr("A + 1")
    ```
``` {title="Output"}

col(A) + lit(1)
```

The above code will create an expression representing "the column named 'x' incremented by 1". For many APIs, [`sql_expr`](api_docs/sql.html#daft.sql_expr) will actually be applied for you as syntactic sugar!

#### Literals

You may find yourself needing to hardcode a "single value" oftentimes as an expression. Daft provides a [`lit()`]({{ api_path }}/expression_methods/daft.lit.html) helper to do so:

=== "🐍 Python"
    ``` python
    from daft import lit

    # Refers to an expression which always evaluates to 42
    lit(42)
    ```

=== "⚙️ SQL"
    ```python
    # Refers to an expression which always evaluates to 42
    daft.sql_expr("42")
    ```

```{title="Output"}

lit(42)
```
This special :func:`~daft.expressions.lit` expression we just created evaluates always to the value ``42``.

#### Wildcard Expressions

You can create expressions on multiple columns at once using a wildcard. The expression [`col("*")`]({{ api_path }}/expression_methods/daft.col.html)) selects every column in a DataFrame, and you can operate on this expression in the same way as a single column:

=== "🐍 Python"
    ``` python
    import daft
    from daft import col

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

Wildcards also work very well for accessing all members of a struct column:

=== "🐍 Python"
    ``` python

    import daft
    from daft import col

    df = daft.from_pydict({
        "person": [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35}
        ]
    })

    # Access all fields of the 'person' struct
    df.select(col("person.*")).show()
    ```

=== "⚙️ SQL"
    ```python
    import daft

    df = daft.from_pydict({
        "person": [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35}
        ]
    })

    # Access all fields of the 'person' struct using SQL
    daft.sql("SELECT person.* FROM df").show()
    ```

``` {title="Output"}

╭──────────┬───────╮
│ name     ┆ age   │
│ ---      ┆ ---   │
│ String   ┆ Int64 │
╞══════════╪═══════╡
│ Alice    ┆ 30    │
├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ Bob      ┆ 25    │
├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ Charlie  ┆ 35    │
╰──────────┴───────╯
```

In this example, we use the wildcard `*` to access all fields of the `person` struct column. This is equivalent to selecting each field individually (`person.name`, `person.age`), but is more concise and flexible, especially when dealing with structs that have many fields.



### Composing Expressions

#### Numeric Expressions

Since column "A" is an integer, we can run numeric computation such as addition, division and checking its value. Here are some examples where we create new columns using the results of such computations:

=== "🐍 Python"
    ``` python
    # Add 1 to each element in column "A"
    df = df.with_column("A_add_one", df["A"] + 1)

    # Divide each element in column A by 2
    df = df.with_column("A_divide_two", df["A"] / 2.)

    # Check if each element in column A is more than 1
    df = df.with_column("A_gt_1", df["A"] > 1)

    df.collect()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.sql("""
        SELECT
            *,
            A + 1 AS A_add_one,
            A / 2.0 AS A_divide_two,
            A > 1 AS A_gt_1
        FROM df
    """)
    df.collect()
    ```

```{title="Output"}

+---------+-------------+----------------+-----------+
|       A |   A_add_one |   A_divide_two | A_gt_1    |
|   Int64 |       Int64 |        Float64 | Boolean   |
+=========+=============+================+===========+
|       1 |           2 |            0.5 | false     |
+---------+-------------+----------------+-----------+
|       2 |           3 |            1   | true      |
+---------+-------------+----------------+-----------+
|       3 |           4 |            1.5 | true      |
+---------+-------------+----------------+-----------+
(Showing first 3 of 3 rows)
```

Notice that the returned types of these operations are also well-typed according to their input types. For example, calling ``df["A"] > 1`` returns a column of type [`Boolean`](api_docs/datatype.html#daft.DataType.bool).

Both the [`Float`](api_docs/datatype.html#daft.DataType.float32) and [`Int`](api_docs/datatype.html#daft.DataType.int16) types are numeric types, and inherit many of the same arithmetic Expression operations. You may find the full list of numeric operations in the [Expressions API Reference](api_docs/expressions.html#numeric).

#### String Expressions

Daft also lets you have columns of strings in a DataFrame. Let's take a look!

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({"B": ["foo", "bar", "baz"]})
    df.show()
    ```

``` {title="Output"}

+--------+
| B      |
| Utf8   |
+========+
| foo    |
+--------+
| bar    |
+--------+
| baz    |
+--------+
(Showing first 3 rows)
```

Unlike the numeric types, the string type does not support arithmetic operations such as `*` and `/`. The one exception to this is the `+` operator, which is overridden to concatenate two string expressions as is commonly done in Python. Let's try that!

=== "🐍 Python"
    ``` python
    df = df.with_column("B2", df["B"] + "foo")
    df.show()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.sql("SELECT *, B + 'foo' AS B2 FROM df")
    df.show()
    ```

``` {title="Output"}

+--------+--------+
| B      | B2     |
| Utf8   | Utf8   |
+========+========+
| foo    | foofoo |
+--------+--------+
| bar    | barfoo |
+--------+--------+
| baz    | bazfoo |
+--------+--------+
(Showing first 3 rows)
```

There are also many string operators that are accessed through a separate [`.str.*`](api_docs/expressions.html#strings) "method namespace".

For example, to check if each element in column "B" contains the substring "a", we can use the [`.str.contains`]({{ api_path }}/expression_methods/daft.Expression.str.contains.html) method:

=== "🐍 Python"
    ``` python
    df = df.with_column("B2_contains_B", df["B2"].str.contains(df["B"]))
    df.show()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.sql("SELECT *, contains(B2, B) AS B2_contains_B FROM df")
    df.show()
    ```

``` {title="Output"}

+--------+--------+-----------------+
| B      | B2     | B2_contains_B   |
| Utf8   | Utf8   | Boolean         |
+========+========+=================+
| foo    | foofoo | true            |
+--------+--------+-----------------+
| bar    | barfoo | true            |
+--------+--------+-----------------+
| baz    | bazfoo | true            |
+--------+--------+-----------------+
(Showing first 3 rows)
```

You may find a full list of string operations in the [Expressions API Reference](api_docs/expressions.html).

#### URL Expressions

One special case of a String column you may find yourself working with is a column of URL strings.

Daft provides the [`.url.*`](api_docs/expressions.html#url-expressions) method namespace with functionality for working with URL strings. For example, to download data from URLs:

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({
        "urls": [
            "https://www.google.com",
            "s3://daft-public-data/open-images/validation-images/0001eeaf4aed83f9.jpg",
        ],
    })
    df = df.with_column("data", df["urls"].url.download())
    df.collect()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.from_pydict({
        "urls": [
            "https://www.google.com",
            "s3://daft-public-data/open-images/validation-images/0001eeaf4aed83f9.jpg",
        ],
    })
    df = daft.sql("""
        SELECT
            urls,
            url_download(urls) AS data
        FROM df
    """)
    df.collect()
    ```

``` {title="Output"}

+----------------------+----------------------+
| urls                 | data                 |
| Utf8                 | Binary               |
+======================+======================+
| https://www.google.c | b'<!doctype          |
| om                   | html><html           |
|                      | itemscope="" itemtyp |
|                      | e="http://sche...    |
+----------------------+----------------------+
| s3://daft-public-    | b'\xff\xd8\xff\xe0\x |
| data/open-           | 00\x10JFIF\x00\x01\x |
| images/validation-   | 01\x01\x00H\x00H\... |
| images/0001e...      |                      |
+----------------------+----------------------+
(Showing first 2 of 2 rows)
```

This works well for URLs which are HTTP paths to non-HTML files (e.g. jpeg), local filepaths or even paths to a file in an object store such as AWS S3 as well!

#### JSON Expressions

If you have a column of JSON strings, Daft provides the [`.json.*`](api_docs/expressions.html#json) method namespace to run [JQ-style filters](https://stedolan.github.io/jq/manual/) on them. For example, to extract a value from a JSON object:

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({
        "json": [
            '{"a": 1, "b": 2}',
            '{"a": 3, "b": 4}',
        ],
    })
    df = df.with_column("a", df["json"].json.query(".a"))
    df.collect()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.from_pydict({
        "json": [
            '{"a": 1, "b": 2}',
            '{"a": 3, "b": 4}',
        ],
    })
    df = daft.sql("""
        SELECT
            json,
            json_query(json, '.a') AS a
        FROM df
    """)
    df.collect()
    ```

``` {title="Output"}

╭──────────────────┬──────╮
│ json             ┆ a    │
│ ---              ┆ ---  │
│ Utf8             ┆ Utf8 │
╞══════════════════╪══════╡
│ {"a": 1, "b": 2} ┆ 1    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
│ {"a": 3, "b": 4} ┆ 3    │
╰──────────────────┴──────╯

(Showing first 2 of 2 rows)
```

Daft uses [jaq](https://github.com/01mf02/jaq/tree/main) as the underlying executor, so you can find the full list of supported filters in the [jaq documentation](https://github.com/01mf02/jaq/tree/main).

#### Logical Expressions

Logical Expressions are an expression that refers to a column of type [`Boolean`](api_docs/datatype.html#daft.DataType.bool), and can only take on the values True or False.

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({"C": [True, False, True]})
    ```

Daft supports logical operations such as `&` (and) and `|` (or) between logical expressions.

#### Comparisons

Many of the types in Daft support comparisons between expressions that returns a Logical Expression.

For example, here we can compare if each element in column "A" is equal to elements in column "B":

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({"A": [1, 2, 3], "B": [1, 2, 4]})

    df = df.with_column("A_eq_B", df["A"] == df["B"])

    df.collect()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.from_pydict({"A": [1, 2, 3], "B": [1, 2, 4]})

    df = daft.sql("""
        SELECT
            A,
            B,
            A = B AS A_eq_B
        FROM df
    """)

    df.collect()
    ```

```{title="Output"}

╭───────┬───────┬─────────╮
│ A     ┆ B     ┆ A_eq_B  │
│ ---   ┆ ---   ┆ ---     │
│ Int64 ┆ Int64 ┆ Boolean │
╞═══════╪═══════╪═════════╡
│ 1     ┆ 1     ┆ true    │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ 2     ┆ 2     ┆ true    │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ 3     ┆ 4     ┆ false   │
╰───────┴───────┴─────────╯

(Showing first 3 of 3 rows)
```

Other useful comparisons can be found in the [Expressions API Reference](api_docs/expressions.html#numeric).

### If Else Pattern

The [`.if_else()`]({{ api_path }}/expression_methods/daft.Expression.if_else.html) method is a useful expression to have up your sleeve for choosing values between two other expressions based on a logical expression:

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({"A": [1, 2, 3], "B": [0, 2, 4]})

    # Pick values from column A if the value in column A is bigger
    # than the value in column B. Otherwise, pick values from column B.
    df = df.with_column(
        "A_if_bigger_else_B",
        (df["A"] > df["B"]).if_else(df["A"], df["B"]),
    )

    df.collect()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.from_pydict({"A": [1, 2, 3], "B": [0, 2, 4]})

    df = daft.sql("""
        SELECT
            A,
            B,
            CASE
                WHEN A > B THEN A
                ELSE B
            END AS A_if_bigger_else_B
        FROM df
    """)

    df.collect()
    ```

```{title="Output"}

╭───────┬───────┬────────────────────╮
│ A     ┆ B     ┆ A_if_bigger_else_B │
│ ---   ┆ ---   ┆ ---                │
│ Int64 ┆ Int64 ┆ Int64              │
╞═══════╪═══════╪════════════════════╡
│ 1     ┆ 0     ┆ 1                  │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2     ┆ 2     ┆ 2                  │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 3     ┆ 4     ┆ 4                  │
╰───────┴───────┴────────────────────╯

(Showing first 3 of 3 rows)
```

This is a useful expression for cleaning your data!


#### Temporal Expressions

Daft provides rich support for working with temporal data types like Timestamp and Duration. Let's explore some common temporal operations:

##### Basic Temporal Operations

You can perform arithmetic operations with timestamps and durations, such as adding a duration to a timestamp or calculating the duration between two timestamps:

=== "🐍 Python"
    ``` python
    import datetime

    df = daft.from_pydict({
        "timestamp": [
            datetime.datetime(2021, 1, 1, 0, 1, 1),
            datetime.datetime(2021, 1, 1, 0, 1, 59),
            datetime.datetime(2021, 1, 1, 0, 2, 0),
        ]
    })

    # Add 10 seconds to each timestamp
    df = df.with_column(
        "plus_10_seconds",
        df["timestamp"] + datetime.timedelta(seconds=10)
    )

    df.show()
    ```

=== "⚙️ SQL"
    ```python
    import datetime

    df = daft.from_pydict({
        "timestamp": [
            datetime.datetime(2021, 1, 1, 0, 1, 1),
            datetime.datetime(2021, 1, 1, 0, 1, 59),
            datetime.datetime(2021, 1, 1, 0, 2, 0),
        ]
    })

    # Add 10 seconds to each timestamp and calculate duration between timestamps
    df = daft.sql("""
        SELECT
            timestamp,
            timestamp + INTERVAL '10 seconds' as plus_10_seconds,
        FROM df
    """)

    df.show()
    ```

``` {title="Output"}

╭───────────────────────────────┬───────────────────────────────╮
│ timestamp                     ┆ plus_10_seconds               │
│ ---                           ┆ ---                           │
│ Timestamp(Microseconds, None) ┆ Timestamp(Microseconds, None) │
╞═══════════════════════════════╪═══════════════════════════════╡
│ 2021-01-01 00:01:01           ┆ 2021-01-01 00:01:11           │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2021-01-01 00:01:59           ┆ 2021-01-01 00:02:09           │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2021-01-01 00:02:00           ┆ 2021-01-01 00:02:10           │
╰───────────────────────────────┴───────────────────────────────╯
```

##### Temporal Component Extraction

The [`.dt.*`](api_docs/expressions.html#temporal) method namespace provides extraction methods for the components of a timestamp, such as year, month, day, hour, minute, and second:

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({
        "timestamp": [
            datetime.datetime(2021, 1, 1, 0, 1, 1),
            datetime.datetime(2021, 1, 1, 0, 1, 59),
            datetime.datetime(2021, 1, 1, 0, 2, 0),
        ]
    })

    # Extract year, month, day, hour, minute, and second from the timestamp
    df = df.with_columns({
        "year": df["timestamp"].dt.year(),
        "month": df["timestamp"].dt.month(),
        "day": df["timestamp"].dt.day(),
        "hour": df["timestamp"].dt.hour(),
        "minute": df["timestamp"].dt.minute(),
        "second": df["timestamp"].dt.second()
    })

    df.show()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.from_pydict({
        "timestamp": [
            datetime.datetime(2021, 1, 1, 0, 1, 1),
            datetime.datetime(2021, 1, 1, 0, 1, 59),
            datetime.datetime(2021, 1, 1, 0, 2, 0),
        ]
    })

    # Extract year, month, day, hour, minute, and second from the timestamp
    df = daft.sql("""
        SELECT
            timestamp,
            year(timestamp) as year,
            month(timestamp) as month,
            day(timestamp) as day,
            hour(timestamp) as hour,
            minute(timestamp) as minute,
            second(timestamp) as second
        FROM df
    """)

    df.show()
    ```

``` {title="Output"}

╭───────────────────────────────┬───────┬────────┬────────┬────────┬────────┬────────╮
│ timestamp                     ┆ year  ┆ month  ┆ day    ┆ hour   ┆ minute ┆ second │
│ ---                           ┆ ---   ┆ ---    ┆ ---    ┆ ---    ┆ ---    ┆ ---    │
│ Timestamp(Microseconds, None) ┆ Int32 ┆ UInt32 ┆ UInt32 ┆ UInt32 ┆ UInt32 ┆ UInt32 │
╞═══════════════════════════════╪═══════╪════════╪════════╪════════╪════════╪════════╡
│ 2021-01-01 00:01:01           ┆ 2021  ┆ 1      ┆ 1      ┆ 0      ┆ 1      ┆ 1      │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ 2021-01-01 00:01:59           ┆ 2021  ┆ 1      ┆ 1      ┆ 0      ┆ 1      ┆ 59     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ 2021-01-01 00:02:00           ┆ 2021  ┆ 1      ┆ 1      ┆ 0      ┆ 2      ┆ 0      │
╰───────────────────────────────┴───────┴────────┴────────┴────────┴────────┴────────╯
```

##### Time Zone Operations

You can parse strings as timestamps with time zones and convert between different time zones:

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({
        "timestamp_str": [
            "2021-01-01 00:00:00.123 +0800",
            "2021-01-02 12:30:00.456 +0800"
        ]
    })

    # Parse the timestamp string with time zone and convert to New York time
    df = df.with_column(
        "ny_time",
        df["timestamp_str"].str.to_datetime(
            "%Y-%m-%d %H:%M:%S%.3f %z",
            timezone="America/New_York"
        )
    )

    df.show()
    ```

=== "⚙️ SQL"
    ```python
    df = daft.from_pydict({
        "timestamp_str": [
            "2021-01-01 00:00:00.123 +0800",
            "2021-01-02 12:30:00.456 +0800"
        ]
    })

    # Parse the timestamp string with time zone and convert to New York time
    df = daft.sql("""
        SELECT
            timestamp_str,
            to_datetime(timestamp_str, '%Y-%m-%d %H:%M:%S%.3f %z', 'America/New_York') as ny_time
        FROM df
    """)

    df.show()
    ```

``` {title="Output"}

╭───────────────────────────────┬───────────────────────────────────────────────────╮
│ timestamp_str                 ┆ ny_time                                           │
│ ---                           ┆ ---                                               │
│ Utf8                          ┆ Timestamp(Milliseconds, Some("America/New_York")) │
╞═══════════════════════════════╪═══════════════════════════════════════════════════╡
│ 2021-01-01 00:00:00.123 +0800 ┆ 2020-12-31 11:00:00.123 EST                       │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2021-01-02 12:30:00.456 +0800 ┆ 2021-01-01 23:30:00.456 EST                       │
╰───────────────────────────────┴───────────────────────────────────────────────────╯
```

##### Temporal Truncation

The [`.dt.truncate()`]({{ api_path }}/expression_methods/daft.Expression.dt.truncate.html) method allows you to truncate timestamps to specific time units. This can be useful for grouping data by time periods. For example, to truncate timestamps to the nearest hour:

=== "🐍 Python"
    ``` python
    df = daft.from_pydict({
        "timestamp": [
            datetime.datetime(2021, 1, 7, 0, 1, 1),
            datetime.datetime(2021, 1, 8, 0, 1, 59),
            datetime.datetime(2021, 1, 9, 0, 30, 0),
            datetime.datetime(2021, 1, 10, 1, 59, 59),
        ]
    })

    # Truncate timestamps to the nearest hour
    df = df.with_column(
        "hour_start",
        df["timestamp"].dt.truncate("1 hour")
    )

    df.show()
    ```

``` {title="Output"}

╭───────────────────────────────┬───────────────────────────────╮
│ timestamp                     ┆ hour_start                    │
│ ---                           ┆ ---                           │
│ Timestamp(Microseconds, None) ┆ Timestamp(Microseconds, None) │
╞═══════════════════════════════╪═══════════════════════════════╡
│ 2021-01-07 00:01:01           ┆ 2021-01-07 00:00:00           │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2021-01-08 00:01:59           ┆ 2021-01-08 00:00:00           │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2021-01-09 00:30:00           ┆ 2021-01-09 00:00:00           │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2021-01-10 01:59:59           ┆ 2021-01-10 01:00:00           │
╰───────────────────────────────┴───────────────────────────────╯
```

<!-- todo(docs - jay): Should this section also have sql examples? -->

Daft can read data from a variety of sources, and write data to many destinations.

## Reading Data

### From Files

DataFrames can be loaded from file(s) on some filesystem, commonly your local filesystem or a remote cloud object store such as AWS S3.

Additionally, Daft can read data from a variety of container file formats, including CSV, line-delimited JSON and Parquet.

Daft supports file paths to a single file, a directory of files, and wildcards. It also supports paths to remote object storage such as AWS S3.
=== "🐍 Python"
    ```python
    import daft

    # You can read a single CSV file from your local filesystem
    df = daft.read_csv("path/to/file.csv")

    # You can also read folders of CSV files, or include wildcards to select for patterns of file paths
    df = daft.read_csv("path/to/*.csv")

    # Other formats such as parquet and line-delimited JSON are also supported
    df = daft.read_parquet("path/to/*.parquet")
    df = daft.read_json("path/to/*.json")

    # Remote filesystems such as AWS S3 are also supported, and can be specified with their protocols
    df = daft.read_csv("s3://mybucket/path/to/*.csv")
    ```

To learn more about each of these constructors, as well as the options that they support, consult the API documentation on [`creating DataFrames from files`](api_docs/creation.html#df-io-files).

### From Data Catalogs

If you use catalogs such as [Apache Iceberg](integrations/iceberg.md) or [Apache Hudi](integrations/hudi.md), you can check out their dedicated integration pages.

### From File Paths

Daft also provides an easy utility to create a DataFrame from globbing a path. You can use the [`daft.from_glob_path`]({{ api_path }}/io_functions/daft.from_glob_path.html) method which will read a DataFrame of globbed filepaths.

=== "🐍 Python"
    ``` python
    df = daft.from_glob_path("s3://mybucket/path/to/images/*.jpeg")

    # +----------+------+-----+
    # | name     | size | ... |
    # +----------+------+-----+
    #   ...
    ```

This is especially useful for reading things such as a folder of images or documents into Daft. A common pattern is to then download data from these files into your DataFrame as bytes, using the [`.url.download()`]({{ api_path }}/expression_methods/daft.Expression.url.download.html) method.


### From Memory

For testing, or small datasets that fit in memory, you may also create DataFrames using Python lists and dictionaries.

=== "🐍 Python"
    ``` python
    # Create DataFrame using a dictionary of {column_name: list_of_values}
    df = daft.from_pydict({"A": [1, 2, 3], "B": ["foo", "bar", "baz"]})

    # Create DataFrame using a list of rows, where each row is a dictionary of {column_name: value}
    df = daft.from_pylist([{"A": 1, "B": "foo"}, {"A": 2, "B": "bar"}, {"A": 3, "B": "baz"}])
    ```

To learn more, consult the API documentation on [`creating DataFrames from in-memory data structures`](api_docs/creation.html#df-io-in-memory).

### From Databases

Daft can also read data from a variety of databases, including PostgreSQL, MySQL, Trino, and SQLite using the [`daft.read_sql`]({{ api_path }}/io_functions/daft.read_sql.html#daft.read_sql) method. In order to partition the data, you can specify a partition column, which will allow Daft to read the data in parallel.

=== "🐍 Python"
    ``` python
    # Read from a PostgreSQL database
    uri = "postgresql://user:password@host:port/database"
    df = daft.read_sql("SELECT * FROM my_table", uri)

    # Read with a partition column
    df = daft.read_sql("SELECT * FROM my_table", partition_col="date", uri)
    ```

To learn more, consult the [`SQL Integration Page`](integrations/sql.md) or the API documentation on [`daft.read_sql`]({{ api_path }}/io_functions/daft.read_sql.html#daft.read_sql).

### Reading a column of URLs

Daft provides a convenient way to read data from a column of URLs using the [`.url.download()`]({{ api_path }}/expression_methods/daft.Expression.url.download.html#daft.Expression.url.download) method. This is particularly useful when you have a DataFrame with a column containing URLs pointing to external resources that you want to fetch and incorporate into your DataFrame.

Here's an example of how to use this feature:

=== "🐍 Python"
    ```python
    # Assume we have a DataFrame with a column named 'image_urls'
    df = daft.from_pydict({
        "image_urls": [
            "https://example.com/image1.jpg",
            "https://example.com/image2.jpg",
            "https://example.com/image3.jpg"
        ]
    })

    # Download the content from the URLs and create a new column 'image_data'
    df = df.with_column("image_data", df["image_urls"].url.download())
    df.show()
    ```

``` {title="Output"}

+------------------------------------+------------------------------------+
| image_urls                         | image_data                         |
| Utf8                               | Binary                             |
+====================================+====================================+
| https://example.com/image1.jpg     | b'\xff\xd8\xff\xe0\x00\x10JFIF...' |
+------------------------------------+------------------------------------+
| https://example.com/image2.jpg     | b'\xff\xd8\xff\xe0\x00\x10JFIF...' |
+------------------------------------+------------------------------------+
| https://example.com/image3.jpg     | b'\xff\xd8\xff\xe0\x00\x10JFIF...' |
+------------------------------------+------------------------------------+

(Showing first 3 of 3 rows)
```

This approach allows you to efficiently download and process data from a large number of URLs in parallel, leveraging Daft's distributed computing capabilities.

## Writing Data

Writing data will execute your DataFrame and write the results out to the specified backend. The [`df.write_*(...)`](api_docs/dataframe.html#df-write-data) methods are used to write DataFrames to files or other destinations.

=== "🐍 Python"
    ``` python
    # Write to various file formats in a local folder
    df.write_csv("path/to/folder/")
    df.write_parquet("path/to/folder/")

    # Write DataFrame to a remote filesystem such as AWS S3
    df.write_csv("s3://mybucket/path/")
    ```

!!! note "Note"

    Because Daft is a distributed DataFrame library, by default it will produce multiple files (one per partition) at your specified destination. Writing your dataframe is a **blocking** operation that executes your DataFrame. It will return a new `DataFrame` that contains the filepaths to the written data.

## DataTypes

All columns in a Daft DataFrame have a DataType (also often abbreviated as `dtype`).

All elements of a column are of the same dtype, or they can be the special Null value (indicating a missing value).

Daft provides simple DataTypes that are ubiquituous in many DataFrames such as numbers, strings and dates - all the way up to more complex types like tensors and images.

!!! tip "Tip"

    For a full overview on all the DataTypes that Daft supports, see the [DataType API Reference](api_docs/datatype.html).


### Numeric DataTypes

Numeric DataTypes allows Daft to represent numbers. These numbers can differ in terms of the number of bits used to represent them (8, 16, 32 or 64 bits) and the semantic meaning of those bits
(float vs integer vs unsigned integers).

Examples:

1. [`DataType.int8()`](api_docs/datatype.html#daft.DataType.int8): represents an 8-bit signed integer (-128 to 127)
2. [`DataType.float32()`](api_docs/datatype.html#daft.DataType.float32): represents a 32-bit float (a float number with about 7 decimal digits of precision)

Columns/expressions with these datatypes can be operated on with many numeric expressions such as `+` and `*`.

See also: [Numeric Expressions](core_concepts.md#numeric-expressions)

### Logical DataTypes

The [`Boolean`](api_docs/datatype.html#daft.DataType.bool) DataType represents values which are boolean values: `True`, `False` or `Null`.

Columns/expressions with this dtype can be operated on using logical expressions such as ``&`` and [`.if_else()`]({{ api_path }}/expression_methods/daft.Expression.if_else.html).

See also: [Logical Expressions](core_concepts.md#logical-expressions)

### String Types

Daft has string types, which represent a variable-length string of characters.

As a convenience method, string types also support the `+` Expression, which has been overloaded to support concatenation of elements between two [`DataType.string()`](api_docs/datatype.html#daft.DataType.string) columns.

1. [`DataType.string()`](api_docs/datatype.html#daft.DataType.string): represents a string of UTF-8 characters
2. [`DataType.binary()`](api_docs/datatype.html#daft.DataType.binary): represents a string of bytes

See also: [String Expressions](core_concepts.md#string-expressions)

### Temporal DataTypes

Temporal DataTypes represent data that have to do with time.

Examples:

1. [`DataType.date()`](api_docs/datatype.html#daft.DataType.date): represents a Date (year, month and day)
2. [`DataType.timestamp()`](api_docs/datatype.html#daft.DataType.timestamp): represents a Timestamp (particular instance in time)

See also: [Temporal Expressions](core_concepts.md#temporal-expressions)

### Nested DataTypes

Nested DataTypes wrap other DataTypes, allowing you to compose types into complex data structures.

Examples:

1. [`DataType.list(child_dtype)`](api_docs/datatype.html#daft.DataType.list): represents a list where each element is of the child `dtype`
2. [`DataType.struct({"field_name": child_dtype})`](api_docs/datatype.html#daft.DataType.struct): represents a structure that has children `dtype`s, each mapped to a field name

### Python DataType

The [`DataType.python()`](api_docs/datatype.html#daft.DataType.python) DataType represent items that are Python objects.

!!! warning "Warning"

    Daft does not impose any invariants about what *Python types* these objects are. To Daft, these are just generic Python objects!

Python is AWESOME because it's so flexible, but it's also slow and memory inefficient! Thus we recommend:

1. **Cast early!**: Casting your Python data into native Daft DataTypes if possible - this results in much more efficient downstream data serialization and computation.
2. **Use Python UDFs**: If there is no suitable Daft representation for your Python objects, use Python UDFs to process your Python data and extract the relevant data to be returned as native Daft DataTypes!

!!! note "Note"

    If you work with Python classes for a generalizable use-case (e.g. documents, protobufs), it may be that these types are good candidates for "promotion" into a native Daft type! Please get in touch with the Daft team and we would love to work together on building your type into canonical Daft types.

### Complex DataTypes

Daft supports many more interesting complex DataTypes, for example:

* [`DataType.tensor()`](api_docs/datatype.html#daft.DataType.tensor): Multi-dimensional (potentially uniformly-shaped) tensors of data
* [`DataType.embedding()`](api_docs/datatype.html#daft.DataType.embedding): Lower-dimensional vector representation of data (e.g. words)
* [`DataType.image()`](api_docs/datatype.html#daft.DataType.image): NHWC images

Daft abstracts away the in-memory representation of your data and provides kernels for many common operations on top of these data types. For supported image operations see the [image expressions API reference](api_docs/expressions.html#api-expressions-images). For more complex algorithms, you can also drop into a Python UDF to process this data using your custom Python libraries.

Please add suggestions for new DataTypes to our Github Discussions page!

## SQL

Daft supports Structured Query Language (SQL) as a way of constructing query plans (represented in Python as a [`daft.DataFrame`]({{ api_path }}/dataframe_methods/daft.DataFrame.html)) and expressions ([`daft.Expression`]({{ api_path }}/dataframe_methods/daft.DataFrame.html)).

SQL is a human-readable way of constructing these query plans, and can often be more ergonomic than using DataFrames for writing queries.

!!! tip "Daft's SQL support is new and is constantly being improved on!"

    Please give us feedback or submit an [issue](https://github.com/Eventual-Inc/Daft/issues) and we'd love to hear more about what you would like.


### Running SQL on DataFrames

Daft's [`daft.sql`](api_docs/sql.html#daft.sql) function will automatically detect any [`daft.DataFrame`]({{ api_path }}/dataframe_methods/daft.DataFrame.html) objects in your current Python environment to let you query them easily by name.

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

In the above example, we query the DataFrame called `"my_special_df"` by simply referring to it in the SQL command. This produces a new DataFrame `sql_df` which can
natively integrate with the rest of your Daft query.

### Reading data from SQL

!!! warning "Warning"

    This feature is a WIP and will be coming soon! We will support reading common datasources directly from SQL:

    === "🐍 Python"

        ```python
        daft.sql("SELECT * FROM read_parquet('s3://...')")
        daft.sql("SELECT * FROM read_delta_lake('s3://...')")
        ```

    Today, a workaround for this is to construct your dataframe in Python first and use it from SQL instead:

    === "🐍 Python"

        ```python
        df = daft.read_parquet("s3://...")
        daft.sql("SELECT * FROM df")
        ```

    We appreciate your patience with us and hope to deliver this crucial feature soon!

### SQL Expressions

SQL has the concept of expressions as well. Here is an example of a simple addition expression, adding columns "a" and "b" in SQL to produce a new column C.

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

One really cool trick you can do is to use the [`daft.sql_expr`](api_docs/sql.html#daft.sql_expr) function as a helper to easily create Expressions. The following are equivalent:

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

SQL also has access to all of Daft's powerful [`daft.Expression`]({{ api_path }}/dataframe_methods/daft.DataFrame.html#daft.DataFrame) functionality through SQL functions.

However, unlike the Python Expression API which encourages method-chaining (e.g. `col("a").url.download().image.decode()`), in SQL you have to do function nesting instead (e.g. `"image_decode(url_download(a))"`).

!!! note "Note"

    A full catalog of the available SQL Functions in Daft is available in the [`SQL API Docs`](api_docs/sql.html).

    Note that it closely mirrors the Python API, with some function naming differences vs the available Python methods.
    We also have some aliased functions for ANSI SQL-compliance or familiarity to users coming from other common SQL dialects such as PostgreSQL and SparkSQL to easily find their functionality.

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

## Aggregations and Grouping

Some operations such as the sum or the average of a column are called **aggregations**. Aggregations are operations that reduce the number of rows in a column.

### Global Aggregations

An aggregation can be applied on an entire DataFrame, for example to get the mean on a specific column:

=== "🐍 Python"
    ``` python
    import daft

    df = daft.from_pydict({
        "class": ["a", "a", "b", "b"],
        "score": [10, 20., 30., 40],
    })

    df.mean("score").show()
    ```

``` {title="Output"}

╭─────────╮
│ score   │
│ ---     │
│ Float64 │
╞═════════╡
│ 25      │
╰─────────╯

(Showing first 1 of 1 rows)
```

For a full list of available Dataframe aggregations, see [Aggregations](api_docs/dataframe.html#df-aggregations).

Aggregations can also be mixed and matched across columns, via the `agg` method:

=== "🐍 Python"
    ``` python
    df.agg(
        df["score"].mean().alias("mean_score"),
        df["score"].max().alias("max_score"),
        df["class"].count().alias("class_count"),
    ).show()
    ```

``` {title="Output"}

╭────────────┬───────────┬─────────────╮
│ mean_score ┆ max_score ┆ class_count │
│ ---        ┆ ---       ┆ ---         │
│ Float64    ┆ Float64   ┆ UInt64      │
╞════════════╪═══════════╪═════════════╡
│ 25         ┆ 40        ┆ 4           │
╰────────────┴───────────┴─────────────╯

(Showing first 1 of 1 rows)
```

For a full list of available aggregation expressions, see [Aggregation Expressions](api_docs/expressions.html#api-aggregation-expression)

### Grouped Aggregations

Aggregations can also be called on a "Grouped DataFrame". For the above example, perhaps we want to get the mean "score" not for the entire DataFrame, but for each "class".

Let's run the mean of column "score" again, but this time grouped by "class":

=== "🐍 Python"
    ``` python
    df.groupby("class").mean("score").show()
    ```

``` {title="Output"}

╭───────┬─────────╮
│ class ┆ score   │
│ ---   ┆ ---     │
│ Utf8  ┆ Float64 │
╞═══════╪═════════╡
│ a     ┆ 15      │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ b     ┆ 35      │
╰───────┴─────────╯

(Showing first 2 of 2 rows)
```

To run multiple aggregations on a Grouped DataFrame, you can use the `agg` method:

=== "🐍 Python"
    ``` python
    df.groupby("class").agg(
        df["score"].mean().alias("mean_score"),
        df["score"].max().alias("max_score"),
    ).show()
    ```

``` {title="Output"}

╭───────┬────────────┬───────────╮
│ class ┆ mean_score ┆ max_score │
│ ---   ┆ ---        ┆ ---       │
│ Utf8  ┆ Float64    ┆ Float64   │
╞═══════╪════════════╪═══════════╡
│ a     ┆ 15         ┆ 20        │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ b     ┆ 35         ┆ 40        │
╰───────┴────────────┴───────────╯

(Showing first 2 of 2 rows)
```

## User-Defined Functions (UDF)

A key piece of functionality in Daft is the ability to flexibly define custom functions that can run computations on any data in your dataframe. This section walks you through the different types of UDFs that Daft allows you to run.

Let's first create a dataframe that will be used as a running example throughout this tutorial!

=== "🐍 Python"
    ``` python
    import daft
    import numpy as np

    df = daft.from_pydict({
        # the `image` column contains images represented as 2D numpy arrays
        "image": [np.ones((128, 128)) for i in range(16)],
        # the `crop` column contains a box to crop from our image, represented as a list of integers: [x1, x2, y1, y2]
        "crop": [[0, 1, 0, 1] for i in range(16)],
    })
    ```


### Per-column per-row functions using [`.apply`]({{ api_path }}/expression_methods/daft.Expression.apply.html)

You can use [`.apply`]({{ api_path }}/expression_methods/daft.Expression.apply.html) to run a Python function on every row in a column.

For example, the following example creates a new `flattened_image` column by calling `.flatten()` on every object in the `image` column.

=== "🐍 Python"
    ``` python
    df.with_column(
        "flattened_image",
        df["image"].apply(lambda img: img.flatten(), return_dtype=daft.DataType.python())
    ).show(2)
    ```

``` {title="Output"}

+----------------------+---------------+---------------------+
| image                | crop          | flattened_image     |
| Python               | List[Int64]   | Python              |
+======================+===============+=====================+
| [[1. 1. 1. ... 1. 1. | [0, 1, 0, 1]  | [1. 1. 1. ... 1. 1. |
| 1.]  [1. 1. 1. ...   |               | 1.]                 |
| 1. 1. 1.]  [1. 1.... |               |                     |
+----------------------+---------------+---------------------+
| [[1. 1. 1. ... 1. 1. | [0, 1, 0, 1]  | [1. 1. 1. ... 1. 1. |
| 1.]  [1. 1. 1. ...   |               | 1.]                 |
| 1. 1. 1.]  [1. 1.... |               |                     |
+----------------------+---------------+---------------------+
(Showing first 2 rows)
```

Note here that we use the `return_dtype` keyword argument to specify that our returned column type is a Python column!

### Multi-column per-partition functions using [`@udf`](api_docs/udf.html#creating-udfs)

[`.apply`]({{ api_path }}/expression_methods/daft.Expression.apply.html) is great for convenience, but has two main limitations:

1. It can only run on single columns
2. It can only run on single items at a time

Daft provides the [`@udf`](api_docs/udf.html#creating-udfs) decorator for defining your own UDFs that process multiple columns or multiple rows at a time.

For example, let's try writing a function that will crop all our images in the `image` column by its corresponding value in the `crop` column:

=== "🐍 Python"
    ``` python
    @daft.udf(return_dtype=daft.DataType.python())
    def crop_images(images, crops, padding=0):
        cropped = []
        for img, crop in zip(images.to_pylist(), crops.to_pylist()):
            x1, x2, y1, y2 = crop
            cropped_img = img[x1:x2 + padding, y1:y2 + padding]
            cropped.append(cropped_img)
        return cropped

    df = df.with_column(
        "cropped",
        crop_images(df["image"], df["crop"], padding=1),
    )
    df.show(2)
    ```

``` {title="Output"}

+----------------------+---------------+--------------------+
| image                | crop          | cropped            |
| Python               | List[Int64]   | Python             |
+======================+===============+====================+
| [[1. 1. 1. ... 1. 1. | [0, 1, 0, 1]  | [[1. 1.]  [1. 1.]] |
| 1.]  [1. 1. 1. ...   |               |                    |
| 1. 1. 1.]  [1. 1.... |               |                    |
+----------------------+---------------+--------------------+
| [[1. 1. 1. ... 1. 1. | [0, 1, 0, 1]  | [[1. 1.]  [1. 1.]] |
| 1.]  [1. 1. 1. ...   |               |                    |
| 1. 1. 1.]  [1. 1.... |               |                    |
+----------------------+---------------+--------------------+
(Showing first 2 rows)
```

There's a few things happening here, let's break it down:

1. `crop_images` is a normal Python function. It takes as input:

    a. A list of images: `images`

    b. A list of cropping boxes: `crops`

    c. An integer indicating how much padding to apply to the right and bottom of the cropping: `padding`

2. To allow Daft to pass column data into the `images` and `crops` arguments, we decorate the function with [`@udf`](api_docs/udf.html#creating-udfs)

    a. `return_dtype` defines the returned data type. In this case, we return a column containing Python objects of numpy arrays

    b. At runtime, because we call the UDF on the `image` and `crop` columns, the UDF will receive a [`daft.Series`](api_docs/series.html) object for each argument.

3. We can create a new column in our DataFrame by applying our UDF on the `"image"` and `"crop"` columns inside of a [`df.with_column()`]({{ api_path }}/dataframe_methods/daft.DataFrame.with_column.html) call.

#### UDF Inputs

When you specify an Expression as an input to a UDF, Daft will calculate the result of that Expression and pass it into your function as a [`daft.Series`](api_docs/series.html) object.

The Daft [`daft.Series`](api_docs/series.html) is just an abstraction on a "column" of data! You can obtain several different data representations from a [`daft.Series`](api_docs/series.html#daft.Series):

1. PyArrow Arrays (`pa.Array`): [`s.to_arrow()`](api_docs/series.html#daft.Series.to_arrow)
2. Python lists (`list`): [`s.to_pylist()`](api_docs/series.html#daft.Series.to_pylist)

Depending on your application, you may choose a different data representation that is more performant or more convenient!

!!! info "Info"

    Certain array formats have some restrictions around the type of data that they can handle:

    1. **Null Handling**: In Pandas and Numpy, nulls are represented as NaNs for numeric types, and Nones for non-numeric types. Additionally, the existence of nulls will trigger a type casting from integer to float arrays. If null handling is important to your use-case, we recommend using one of the other available options.

    2. **Python Objects**: PyArrow array formats cannot support Python columns.

    We recommend using Python lists if performance is not a major consideration, and using the arrow-native formats such as PyArrow arrays and numpy arrays if performance is important.

#### Return Types

The `return_dtype` argument specifies what type of column your UDF will return. Types can be specified using the [`daft.DataType`](api_docs/datatype.html#daft.DataType) class.

Your UDF function itself needs to return a batch of columnar data, and can do so as any one of the following array types:

1. Numpy Arrays (`np.ndarray`)
2. PyArrow Arrays (`pa.Array`)
3. Python lists (`list`)

Note that if the data you have returned is not castable to the return_dtype that you specify (e.g. if you return a list of floats when you've specified a `return_dtype=DataType.bool()`), Daft will throw a runtime error!

### Class UDFs

UDFs can also be created on Classes, which allow for initialization on some expensive state that can be shared between invocations of the class, for example downloading data or creating a model.

=== "🐍 Python"
    ``` python
    @daft.udf(return_dtype=daft.DataType.int64())
    class RunModel:

        def __init__(self):
            # Perform expensive initializations
            self._model = create_model()

        def __call__(self, features_col):
            return self._model(features_col)
    ```

Running Class UDFs are exactly the same as running their functional cousins.

=== "🐍 Python"
    ``` python
    df = df.with_column("image_classifications", RunModel(df["images"]))
    ```

### Resource Requests

Sometimes, you may want to request for specific resources for your UDF. For example, some UDFs need one GPU to run as they will load a model onto the GPU.

To do so, you can create your UDF and assign it a resource request:

=== "🐍 Python"
    ``` python
    @daft.udf(return_dtype=daft.DataType.int64(), num_gpus=1)
    class RunModelWithOneGPU:

        def __init__(self):
            # Perform expensive initializations
            self._model = create_model()

        def __call__(self, features_col):
            return self._model(features_col)
    ```

    ``` python
    df = df.with_column(
        "image_classifications",
        RunModelWithOneGPU(df["images"]),
    )
    ```

In the above example, if Daft ran on a Ray cluster consisting of 8 GPUs and 64 CPUs, Daft would be able to run 8 replicas of your UDF in parallel, thus massively increasing the throughput of your UDF!

UDFs can also be parametrized with new resource requests after being initialized.

=== "🐍 Python"
    ``` python
    RunModelWithTwoGPUs = RunModelWithOneGPU.override_options(num_gpus=2)
    df = df.with_column(
        "image_classifications",
        RunModelWithTwoGPUs(df["images"]),
    )
    ```

### Example: UDFs in ML Workloads

We’ll define a function that uses a pre-trained PyTorch model: [ResNet50](https://pytorch.org/vision/main/models/generated/torchvision.models.resnet50.html) to classify the dog pictures. We’ll pass the contents of the image `urls` column and send the classification predictions to a new column `classify_breed`.

Working with PyTorch adds some complexity but you can just run the cells below to perform the classification.

First, make sure to install and import some extra dependencies:

```bash

%pip install validators matplotlib Pillow torch torchvision

```

=== "🐍 Python"

    ```python
    # import additional libraries, these are necessary for PyTorch
    import torch
    ```

Define your `ClassifyImages` UDF. Models are expensive to initialize and load, so we want to do this as few times as possible, and share a model across multiple invocations.

=== "🐍 Python"

    ```python
    @udf(return_dtype=DataType.fixed_size_list(dtype=DataType.string(), size=2))
    class ClassifyImages:
        def __init__(self):
            # Perform expensive initializations - create and load the pre-trained model
            self.model = torch.hub.load("NVIDIA/DeepLearningExamples:torchhub", "nvidia_resnet50", pretrained=True)
            self.utils = torch.hub.load("NVIDIA/DeepLearningExamples:torchhub", "nvidia_convnets_processing_utils")
            self.model.eval().to(torch.device("cpu"))

        def __call__(self, images_urls):
            uris = images_urls.to_pylist()
            batch = torch.cat([self.utils.prepare_input_from_uri(uri) for uri in uris]).to(torch.device("cpu"))

            with torch.no_grad():
                output = torch.nn.functional.softmax(self.model(batch), dim=1)

            results = self.utils.pick_n_best(predictions=output, n=1)
            return [result[0] for result in results]
    ```

Now you're ready to call this function on the `urls` column and store the outputs in a new column we'll call `classify_breed`:

=== "🐍 Python"

    ```python
    classified_images_df = df_family.with_column("classify_breed", ClassifyImages(daft.col("urls")))
    classified_images_df.select("dog_name", "image", "classify_breed").show()
    ```

<!-- todo(docs - jay): Insert table of dog urls? or new UDF example? This was from the original 10-min quickstart with multimodal -->

## Multimodal Data

Daft is built to work comfortably with multimodal data types, including URLs and images. You can use the [`url.download()`]({{ api_path }}/expression_methods/daft.Expression.url.download.html) expression to download the bytes from a URL. Let’s store them in a new column using the `with_column` method:

=== "🐍 Python"

    ```python
    df_family = df_family.with_column("image_bytes", df_dogs["urls"].url.download(on_error="null"))
    df_family.show()
    ```

```{title="Output"}
+-------------------+---------+----------+------------------------------------------------------------------+--------------------------------------------+
| full_name         | has_dog | dog_name | urls                                                             | image_bytes                                |
| Utf8              | Boolean | Utf8     | Utf8                                                             | Binary                                     |
+-------------------+---------+----------+------------------------------------------------------------------+--------------------------------------------+
| Ernesto Evergreen | true    | Ernie    | https://live.staticflickr.com/65535/53671838774_03ba68d203_o.jpg | b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01"... |
| James Jale        | true    | Jackie   | https://live.staticflickr.com/65535/53671700073_2c9441422e_o.jpg | b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01"... |
| Wolfgang Winter   | true    | Wolfie   | https://live.staticflickr.com/65535/53670606332_1ea5f2ce68_o.jpg | b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01"... |
| Shandra Shamas    | true    | Shaggie  | https://live.staticflickr.com/65535/53671838039_b97411a441_o.jpg | b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01"... |
| Zaya Zaphora      | true    | Zadie    | https://live.staticflickr.com/65535/53671698613_0230f8af3c_o.jpg | b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01"... |
+-------------------+---------+----------+------------------------------------------------------------------+--------------------------------------------+
(Showing first 5 of 5 rows)
```

Let’s turn the bytes into human-readable images using [`image.decode()`]({{ api_path }}/expression_methods/daft.Expression.image.decode.html):

=== "🐍 Python"

    ```python
    df_family = df_family.with_column("image", daft.col("image_bytes").image.decode())
    df_family.show()
    ```

## What's Next?

### Integrations

<div class="grid cards" markdown>

- [:simple-ray: **Ray**](integrations/ray.md)
- [**Unity Catalog**](integrations/unity_catalog.md)
- [**Apache Iceberg**](integrations/iceberg.md)
- [**Delta Lake**](integrations/delta_lake.md)
- [:material-microsoft-azure: **Microsoft Azure**](integrations/azure.md)
- [:fontawesome-brands-aws: **Amazon Web Services (AWS)**](integrations/aws.md)
- [**SQL**](integrations/sql.md)
- [:simple-huggingface: **Hugging Face Datasets**](integrations/huggingface.md)

</div>

### Migration Guide

<div class="grid cards" markdown>

<!-- - [:simple-apachespark: **Coming from Spark**](migratoin/spark_migration.md) -->
- [:simple-dask: **Coming from Dask**](migration/dask_migration.md)

</div>

### Tutorials

<div class="grid cards" markdown>

- [:material-image-edit: **MNIST Digit Classification**](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/mnist.ipynb)
- [:octicons-search-16: **Running LLMs on the Red Pajamas Dataset**](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/embeddings/daft_tutorial_embeddings_stackexchange.ipynb)
- [:material-image-search: **Querying Images with UDFs**](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/image_querying/top_n_red_color.ipynb)
- [:material-image-sync: **Image Generation on GPUs**](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/text_to_image/text_to_image_generation.ipynb)

</div>

### Advanced

<div class="grid cards" markdown>

- [:material-memory: **Managing Memory Usage**](advanced/memory.md)
- [:fontawesome-solid-equals: **Partitioning**](advanced/partitioning.md)
- [:material-distribute-vertical-center: **Distributed Computing**](distributed.md)

</div>
