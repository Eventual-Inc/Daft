# Quickstart

<!--
todo(docs - jay): Incorporate SQL examples

todo(docs): Add link to notebook to DIY (notebook is in mkdocs dir, but idk how to host on colab)

todo(docs): What does the actual output look like for some of these examples? should we update it visually?
-->

In this quickstart, you will learn the basics of Daft's DataFrame and SQL API and the features that set it apart from frameworks like Pandas, PySpark, Dask, and Ray.

<!-- You will build a database of dog owners and their fluffy companions and see how you can use Daft to download images from URLs, run an ML classifier and call custom UDFs, all within an interactive DataFrame interface. Woof! 🐶 -->

### Install Daft

You can install Daft using `pip`. Run the following command in your terminal or notebook:

=== "🐍 Python"

    ```python
    pip install daft
    ```

For more advanced installation options, please see [Installation](install.md).

### Create Your First DataFrame in Daft

See also [I/O API Docs](api/io.md). Let's create a DataFrame from a dictionary of columns:

=== "🐍 Python"
    ```python
    import daft

    df = daft.from_pydict({
        "A": [1, 2, 3, 4],
        "B": [1.5, 2.5, 3.5, 4.5],
        "C": [True, True, False, False],
        "D": [None, None, None, None],
    })

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

You just created your first DataFrame!

### Read From a Data Source

Daft supports both local paths as well as paths to object storage such as AWS S3:

- CSV files: [`daft.read_csv("s3://path/to/bucket/*.csv")`][daft.read_csv]
- Parquet files: [`daft.read_parquet("/path/*.parquet")`][daft.read_parquet]
- JSON line-delimited files: [`daft.read_json("/path/*.json")`][daft.read_json]
- Files on disk: [`daft.from_glob_path("/path/*.jpeg")`][daft.from_glob_path]

!!! tip "Note"

    To work with other formats like [Delta Lake](io/delta_lake.md) and [Iceberg](io/iceberg.md), check out their respective pages.

Let’s read in a Parquet file from a public S3 bucket. Note that this Parquet file is partitioned on the column `country`. This will be important later on.

<!-- todo(docs - jay): SQL equivalent? -->


=== "🐍 Python"
    ```python

    # Set IO Configurations to use anonymous data access mode
    daft.set_planning_config(default_io_config=daft.io.IOConfig(s3=daft.io.S3Config(anonymous=True)))

    df = daft.read_parquet("s3://daft-public-data/tutorials/10-min/sample-data-dog-owners-partitioned.pq/**")
    df
    ```

```{title="Output"}

╭────────────┬───────────┬───────┬──────┬─────────┬─────────╮
│ first_name ┆ last_name ┆ age   ┆ DoB  ┆ country ┆ has_dog │
│ ---        ┆ ---       ┆ ---   ┆ ---  ┆ ---     ┆ ---     │
│ Utf8       ┆ Utf8      ┆ Int64 ┆ Date ┆ Utf8    ┆ Boolean │
╰────────────┴───────────┴───────┴──────┴─────────┴─────────╯

(No data to display: Dataframe not materialized)

```

Why does it say `(No data to display: Dataframe not materialized)` and where are the rows?

### Execute Your DataFrame and View Data

Daft is **lazy** by default. This means that the contents will not be computed (“materialized”) unless you explicitly tell Daft to do so. This is best practice for working with larger-than-memory datasets and parallel/distributed architectures.

The file we have just loaded only has 5 rows. You can materialize the whole DataFrame in memory easily using the [`df.collect()`][daft.DataFrame.collect] method:

<!-- todo(docs - jay): How does SQL materialize the DataFrame? -->

=== "🐍 Python"

    ```python
    df.collect()
    ```

```{title="Output"}

╭────────────┬───────────┬───────┬────────────┬────────────────┬─────────╮
│ first_name ┆ last_name ┆ age   ┆ DoB        ┆ country        ┆ has_dog │
│ ---        ┆ ---       ┆ ---   ┆ ---        ┆ ---            ┆ ---     │
│ Utf8       ┆ Utf8      ┆ Int64 ┆ Date       ┆ Utf8           ┆ Boolean │
╞════════════╪═══════════╪═══════╪════════════╪════════════════╪═════════╡
│ Shandra    ┆ Shamas    ┆ 57    ┆ 1967-01-02 ┆ United Kingdom ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Zaya       ┆ Zaphora   ┆ 40    ┆ 1984-04-07 ┆ United Kingdom ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Wolfgang   ┆ Winter    ┆ 23    ┆ 2001-02-12 ┆ Germany        ┆ None    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Ernesto    ┆ Evergreen ┆ 34    ┆ 1990-04-03 ┆ Canada         ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ James      ┆ Jale      ┆ 62    ┆ 1962-03-24 ┆ Canada         ┆ true    │
╰────────────┴───────────┴───────┴────────────┴────────────────┴─────────╯

(Showing first 5 of 5 rows)
```

To view just the first few rows, you can use the [`df.show()`][daft.DataFrame.show] method:

=== "🐍 Python"

    ```python
    df.show(3)
    ```

```{title="Output"}

╭────────────┬───────────┬───────┬────────────┬────────────────┬─────────╮
│ first_name ┆ last_name ┆ age   ┆ DoB        ┆ country        ┆ has_dog │
│ ---        ┆ ---       ┆ ---   ┆ ---        ┆ ---            ┆ ---     │
│ Utf8       ┆ Utf8      ┆ Int64 ┆ Date       ┆ Utf8           ┆ Boolean │
╞════════════╪═══════════╪═══════╪════════════╪════════════════╪═════════╡
│ Shandra    ┆ Shamas    ┆ 57    ┆ 1967-01-02 ┆ United Kingdom ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Zaya       ┆ Zaphora   ┆ 40    ┆ 1984-04-07 ┆ United Kingdom ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Wolfgang   ┆ Winter    ┆ 23    ┆ 2001-02-12 ┆ Germany        ┆ None    │
╰────────────┴───────────┴───────┴────────────┴────────────────┴─────────╯

(Showing first 3 of 5 rows)

```

Now let's take a look at some common DataFrame operations.

### Select Columns

<!-- todo(docs - jay): SQL equivalent? -->

You can **select** specific columns from your DataFrame with the [`df.select()`][daft.DataFrame.select] method:

=== "🐍 Python"

    ```python
    df.select("first_name", "has_dog").show()
    ```

```{title="Output"}

╭────────────┬─────────╮
│ first_name ┆ has_dog │
│ ---        ┆ ---     │
│ Utf8       ┆ Boolean │
╞════════════╪═════════╡
│ Shandra    ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Zaya       ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Ernesto    ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ James      ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Wolfgang   ┆ None    │
╰────────────┴─────────╯

(Showing first 5 of 5 rows)

```
### Select Rows

You can **filter** rows using the [`df.where()`][daft.DataFrame.where] method that takes an Logical Expression predicate input. In this case, we call the [`df.col()`][daft.col] method that refers to the column with the provided name `age`:

=== "🐍 Python"

    ```python
    df.where(daft.col("age") >= 40).show()
    ```

```{title="Output"}
╭────────────┬───────────┬───────┬────────────┬────────────────┬─────────╮
│ first_name ┆ last_name ┆ age   ┆ DoB        ┆ country        ┆ has_dog │
│ ---        ┆ ---       ┆ ---   ┆ ---        ┆ ---            ┆ ---     │
│ Utf8       ┆ Utf8      ┆ Int64 ┆ Date       ┆ Utf8           ┆ Boolean │
╞════════════╪═══════════╪═══════╪════════════╪════════════════╪═════════╡
│ Shandra    ┆ Shamas    ┆ 57    ┆ 1967-01-02 ┆ United Kingdom ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Zaya       ┆ Zaphora   ┆ 40    ┆ 1984-04-07 ┆ United Kingdom ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ James      ┆ Jale      ┆ 62    ┆ 1962-03-24 ┆ Canada         ┆ true    │
╰────────────┴───────────┴───────┴────────────┴────────────────┴─────────╯

(Showing first 3 of 3 rows)
```

Filtering can give you powerful optimization when you are working with partitioned files or tables. Daft will use the predicate to read only the necessary partitions, skipping any data that is not relevant.

!!! tip "Note"

    As mentioned earlier that our Parquet file is partitioned on the `country` column, this means that queries with a `country` predicate will benefit from query optimization.

### Exclude Data

You can **limit** the number of rows in a DataFrame by calling the [`df.limit()`[daft.DataFrame.limit] method:

=== "🐍 Python"

    ```python
    df.limit(2).show()
    ```

```{title="Output"}
╭────────────┬───────────┬───────┬────────────┬─────────┬─────────╮
│ first_name ┆ last_name ┆ age   ┆ DoB        ┆ country ┆ has_dog │
│ ---        ┆ ---       ┆ ---   ┆ ---        ┆ ---     ┆ ---     │
│ Utf8       ┆ Utf8      ┆ Int64 ┆ Date       ┆ Utf8    ┆ Boolean │
╞════════════╪═══════════╪═══════╪════════════╪═════════╪═════════╡
│ Wolfgang   ┆ Winter    ┆ 23    ┆ 2001-02-12 ┆ Germany ┆ None    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Ernesto    ┆ Evergreen ┆ 34    ┆ 1990-04-03 ┆ Canada  ┆ true    │
╰────────────┴───────────┴───────┴────────────┴─────────┴─────────╯

(Showing first 2 of 2 rows)
```

To **drop** columns from the DataFrame, use the [`df.exclude()`][daft.DataFrame.exclude] method.

=== "🐍 Python"

    ```python
    df.exclude("DoB").show()
    ```

```{title="Output"}
╭────────────┬───────────┬───────┬────────────────┬─────────╮
│ first_name ┆ last_name ┆ age   ┆ country        ┆ has_dog │
│ ---        ┆ ---       ┆ ---   ┆ ---            ┆ ---     │
│ Utf8       ┆ Utf8      ┆ Int64 ┆ Utf8           ┆ Boolean │
╞════════════╪═══════════╪═══════╪════════════════╪═════════╡
│ Ernesto    ┆ Evergreen ┆ 34    ┆ Canada         ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ James      ┆ Jale      ┆ 62    ┆ Canada         ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Shandra    ┆ Shamas    ┆ 57    ┆ United Kingdom ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Zaya       ┆ Zaphora   ┆ 40    ┆ United Kingdom ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Wolfgang   ┆ Winter    ┆ 23    ┆ Germany        ┆ None    │
╰────────────┴───────────┴───────┴────────────────┴─────────╯

(Showing first 5 of 5 rows)
```

### Transform Columns with Expressions

[Expressions](core_concepts.md#expressions) are an API for defining computation that needs to happen over columns. For example, use the [`daft.col()`][daft.col] expressions together with the [`with_column`][daft.DataFrame.with_column] method to create a new column called `full_name`, joining the contents from the `last_name` column with the `first_name` column:

=== "🐍 Python"

    ```python
    df = df.with_column("full_name", daft.col("first_name") + " " + daft.col("last_name"))
    df.select("full_name", "age", "country", "has_dog").show()
    ```

```{title="Output"}
╭───────────────────┬───────┬────────────────┬─────────╮
│ full_name         ┆ age   ┆ country        ┆ has_dog │
│ ---               ┆ ---   ┆ ---            ┆ ---     │
│ Utf8              ┆ Int64 ┆ Utf8           ┆ Boolean │
╞═══════════════════╪═══════╪════════════════╪═════════╡
│ Wolfgang Winter   ┆ 23    ┆ Germany        ┆ None    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Shandra Shamas    ┆ 57    ┆ United Kingdom ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Zaya Zaphora      ┆ 40    ┆ United Kingdom ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Ernesto Evergreen ┆ 34    ┆ Canada         ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ James Jale        ┆ 62    ┆ Canada         ┆ true    │
╰───────────────────┴───────┴────────────────┴─────────╯

(Showing first 5 of 5 rows)
```

Alternatively, you can also run your column transformation using Expressions directly inside your [`df.select()`][daft.DataFrame.select] method*:

=== "🐍 Python"

    ```python
    df.select((daft.col("first_name").alias("full_name") + " " + daft.col("last_name")), "age", "country", "has_dog").show()
    ```

```{title="Output"}
╭───────────────────┬───────┬────────────────┬─────────╮
│ full_name         ┆ age   ┆ country        ┆ has_dog │
│ ---               ┆ ---   ┆ ---            ┆ ---     │
│ Utf8              ┆ Int64 ┆ Utf8           ┆ Boolean │
╞═══════════════════╪═══════╪════════════════╪═════════╡
│ Shandra Shamas    ┆ 57    ┆ United Kingdom ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Zaya Zaphora      ┆ 40    ┆ United Kingdom ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Wolfgang Winter   ┆ 23    ┆ Germany        ┆ None    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Ernesto Evergreen ┆ 34    ┆ Canada         ┆ true    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ James Jale        ┆ 62    ┆ Canada         ┆ true    │
╰───────────────────┴───────┴────────────────┴─────────╯

(Showing first 5 of 5 rows)
```

### Sort Data

You can **sort** a DataFrame with the [`df.sort()`][daft.DataFrame.sort], in this example we chose to sort in ascending order:

=== "🐍 Python"

    ```python
    df.sort(daft.col("age"), desc=False).show()
    ```

```{title="Output"}
╭────────────┬───────────┬───────┬────────────┬────────────────┬─────────┬───────────────────╮
│ first_name ┆ last_name ┆ age   ┆ DoB        ┆ country        ┆ has_dog ┆ full_name         │
│ ---        ┆ ---       ┆ ---   ┆ ---        ┆ ---            ┆ ---     ┆ ---               │
│ Utf8       ┆ Utf8      ┆ Int64 ┆ Date       ┆ Utf8           ┆ Boolean ┆ Utf8              │
╞════════════╪═══════════╪═══════╪════════════╪════════════════╪═════════╪═══════════════════╡
│ Wolfgang   ┆ Winter    ┆ 23    ┆ 2001-02-12 ┆ Germany        ┆ None    ┆ Wolfgang Winter   │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Ernesto    ┆ Evergreen ┆ 34    ┆ 1990-04-03 ┆ Canada         ┆ true    ┆ Ernesto Evergreen │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Zaya       ┆ Zaphora   ┆ 40    ┆ 1984-04-07 ┆ United Kingdom ┆ true    ┆ Zaya Zaphora      │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Shandra    ┆ Shamas    ┆ 57    ┆ 1967-01-02 ┆ United Kingdom ┆ true    ┆ Shandra Shamas    │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ James      ┆ Jale      ┆ 62    ┆ 1962-03-24 ┆ Canada         ┆ true    ┆ James Jale        │
╰────────────┴───────────┴───────┴────────────┴────────────────┴─────────┴───────────────────╯

(Showing first 5 of 5 rows)
```

### Group and Aggregate Data

You can **group** and **aggregate** your data using the [`df.groupby()`][daft.DataFrame.groupby] and the [`df.agg()`][daft.DataFrame.agg] methods. A groupby aggregation operation over a dataset happens in 2 steps:

1. Split the data into groups based on some criteria using [`df.groupby()`][daft.DataFrame.groupby]
2. Specify how to aggregate the data for each group using [`df.agg()`][daft.DataFrame.agg]

=== "🐍 Python"

    ```python
    grouped = df.groupby("country").agg(
        daft.col("age").mean().alias("avg_age"),
        daft.col("has_dog").count()
    ).show()
    ```

```{title="Output"}
╭────────────────┬─────────┬─────────╮
│ country        ┆ avg_age ┆ has_dog │
│ ---            ┆ ---     ┆ ---     │
│ Utf8           ┆ Float64 ┆ UInt64  │
╞════════════════╪═════════╪═════════╡
│ United Kingdom ┆ 48.5    ┆ 2       │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Canada         ┆ 48      ┆ 2       │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ Germany        ┆ 23      ┆ 0       │
╰────────────────┴─────────┴─────────╯

(Showing first 3 of 3 rows)
```

!!! tip "Note"

    The [`df.alias()`][daft.Expression.alias] method renames the given column.


### What's Next?

Now that you have a basic sense of Daft’s functionality and features, here are some more resources to help you get the most out of Daft:

**Check out our [Core Concepts](core_concepts.md) for more details about:**

<div class="grid cards" markdown>

- [:material-filter: **DataFrame Operations**](core_concepts.md#dataframe)
- [:octicons-code-16: **Expressions**](core_concepts.md#expressions)
- [:material-file-eye: **Reading Data**](core_concepts.md#reading-data)
- [:material-file-edit: **Writing Data**](core_concepts.md#reading-data)
- [:fontawesome-solid-square-binary: **DataTypes**](core_concepts.md#datatypes)
- [:simple-quicklook: **SQL**](core_concepts.md#sql)
- [:material-select-group: **Aggregations and Grouping**](core_concepts.md#aggregations-and-grouping)
- [:material-window-closed-variant: **Window Functions**](core_concepts.md#window-functions)
- [:fontawesome-solid-user: **User-Defined Functions (UDFs)**](core_concepts.md#user-defined-functions-udf)
- [:octicons-image-16: **Multimodal Data**](core_concepts.md#multimodal-data)

</div>

**Work with your favorite table and catalog formats**:

<div class="grid cards" markdown>

- [**Apache Hudi**](io/hudi.md)
- [**Apache Iceberg**](io/iceberg.md)
- [**AWS Glue**](catalogs/glue.md)
- [**AWS S3Tables**](catalogs/s3tables.md)
- [**Delta Lake**](io/delta_lake.md)
- [**Hugging Face Datasets**](io/huggingface.md)
- [**Unity Catalog**](catalogs/unity_catalog.md)
<!-- - [**LanceDB**](io/lancedb.md) -->

</div>

**Coming from?**

<div class="grid cards" markdown>

- [:simple-dask: **Dask Migration Guide**](migration/dask_migration.md)

</div>

**Try your hand at some [Tutorials](resources/tutorials.md):**

<div class="grid cards" markdown>

- [:material-image-edit: **MNIST Digit Classification**](resources/tutorials.md#mnist-digit-classification)
- [:octicons-search-16: **Running LLMs on the Red Pajamas Dataset**](resources/tutorials.md#running-llms-on-the-red-pajamas-dataset)
- [:material-image-search: **Querying Images with UDFs**](resources/tutorials.md#querying-images-with-udfs)
- [:material-image-sync: **Image Generation on GPUs**](resources/tutorials.md#image-generation-on-gpus)
- [:material-window-closed-variant: **Window Functions in Daft**](resources/tutorials.md#window-functions)

</div>
