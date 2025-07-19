# DataFrame

Most DataFrame methods are **lazy**, meaning that they do not execute computation immediately when invoked. Instead, these operations are enqueued in the DataFrame's internal query plan, and are only executed when Execution DataFrame methods are called.

Learn more about [DataFrames](../core_concepts.md#dataframe) in Daft User Guide and see also [Aggregations API Reference](aggregations.md).

<div class="grid cards api" markdown>

* [**Selecting Columns**](#selecting-columns)

    Choose specific columns to include or exclude from the DataFrame.

* [**Filtering Rows**](#filtering-rows)

    Remove rows based on conditions, duplicates, null values, or sample subsets.

* [**Manipulating Columns**](#manipulating-columns)

    Add, rename, transform, and reshape columns including pivoting operations.

* [**Reordering**](#reordering)

    Sort DataFrame rows by column values and limit the number of returned rows.

* [**Combining**](#combining)

    Merge multiple DataFrames through joins, unions, intersections, and set operations.

* [**Materialization**](#materialization)

    Execute lazy operations and convert to Arrow, Pandas, or PyTorch formats.

* [**Iteration**](#iteration)

    Iterate through DataFrame contents lazily without full materialization.

* [**Partitioning**](#partitioning)

    Control how data is distributed across partitions for parallel processing.

* [**Visualization**](#visualization)

    Display DataFrame contents and query execution plans for debugging.

* [**Metadata**](#metadata)

    Access DataFrame schema, length, and column information without computation.

* [**Utility**](#utility)

    Apply custom functions to the DataFrame using functional programming patterns.

</div>

## Selecting Columns

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`exclude`][daft.DataFrame.exclude] | Drops columns from the current DataFrame by name. |
| [`select`][daft.DataFrame.select] | Creates a new DataFrame from the provided expressions, similar to a SQL ``SELECT``. |
<!-- END GENERATED TABLE -->

::: daft.DataFrame.exclude
::: daft.DataFrame.select

## Filtering Rows

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`distinct`][daft.DataFrame.distinct] | Computes distinct rows, dropping duplicates. |
| [`drop_duplicates`][daft.DataFrame.drop_duplicates] | Computes distinct rows, dropping duplicates. |
| [`drop_nan`][daft.DataFrame.drop_nan] | Drops rows that contains NaNs. If cols is None it will drop rows with any NaN value. |
| [`drop_null`][daft.DataFrame.drop_null] | Drops rows that contains NaNs or NULLs. If cols is None it will drop rows with any NULL value. |
| [`filter`][daft.DataFrame.filter] | Filters rows via a predicate expression, similar to SQL ``WHERE``. |
| [`limit`][daft.DataFrame.limit] | Limits the rows in the DataFrame to the first ``N`` rows, similar to a SQL ``LIMIT``. |
| [`sample`][daft.DataFrame.sample] | Samples a fraction of rows from the DataFrame. |
| [`unique`][daft.DataFrame.unique] | Computes distinct rows, dropping duplicates. |
| [`where`][daft.DataFrame.where] | Filters rows via a predicate expression, similar to SQL ``WHERE``. |
<!-- END GENERATED TABLE -->

::: daft.DataFrame.distinct
::: daft.DataFrame.drop_duplicates
::: daft.DataFrame.drop_nan
::: daft.DataFrame.drop_null
::: daft.DataFrame.filter
::: daft.DataFrame.limit
::: daft.DataFrame.sample
::: daft.DataFrame.unique
::: daft.DataFrame.where

## Manipulating Columns

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`explode`][daft.DataFrame.explode] | Explodes a List column, where every element in each row's List becomes its own row, and all other columns in the DataFrame are duplicated across rows. |
| [`melt`][daft.DataFrame.melt] | Alias for unpivot. |
| [`pivot`][daft.DataFrame.pivot] | Pivots a column of the DataFrame and performs an aggregation on the values. |
| [`transform`][daft.DataFrame.transform] | Apply a function that takes and returns a DataFrame. |
| [`unpivot`][daft.DataFrame.unpivot] | Unpivots a DataFrame from wide to long format. |
| [`with_column`][daft.DataFrame.with_column] | Adds a column to the current DataFrame with an Expression, equivalent to a ``select`` with all current columns and the new one. |
| [`with_column_renamed`][daft.DataFrame.with_column_renamed] | Renames a column in the current DataFrame. |
| [`with_columns`][daft.DataFrame.with_columns] | Adds columns to the current DataFrame with Expressions, equivalent to a ``select`` with all current columns and the new ones. |
| [`with_columns_renamed`][daft.DataFrame.with_columns_renamed] | Renames multiple columns in the current DataFrame. |
<!-- END GENERATED TABLE -->

::: daft.DataFrame.explode
::: daft.DataFrame.melt
::: daft.DataFrame.pivot
::: daft.DataFrame.transform
::: daft.DataFrame.unpivot
::: daft.DataFrame.with_column
::: daft.DataFrame.with_column_renamed
::: daft.DataFrame.with_columns
::: daft.DataFrame.with_columns_renamed

## Reordering

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`limit`][daft.DataFrame.limit] | Limits the rows in the DataFrame to the first ``N`` rows, similar to a SQL ``LIMIT``. |
| [`sort`][daft.DataFrame.sort] | Sorts DataFrame globally. |
<!-- END GENERATED TABLE -->

::: daft.DataFrame.limit
::: daft.DataFrame.sort

## Combining

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`concat`][daft.DataFrame.concat] | Concatenates two DataFrames together in a "vertical" concatenation. |
| [`except_all`][daft.DataFrame.except_all] | Returns the set difference of two DataFrames, considering duplicates. |
| [`except_distinct`][daft.DataFrame.except_distinct] | Returns the set difference of two DataFrames. |
| [`intersect`][daft.DataFrame.intersect] | Returns the intersection of two DataFrames. |
| [`intersect_all`][daft.DataFrame.intersect_all] | Returns the intersection of two DataFrames, including duplicates. |
| [`join`][daft.DataFrame.join] | Column-wise join of the current DataFrame with an ``other`` DataFrame, similar to a SQL ``JOIN``. |
| [`union`][daft.DataFrame.union] | Returns the distinct union of two DataFrames. |
| [`union_all`][daft.DataFrame.union_all] | Returns the union of two DataFrames, including duplicates. |
| [`union_all_by_name`][daft.DataFrame.union_all_by_name] | Returns the union of two DataFrames, including duplicates, with columns matched by name. |
| [`union_by_name`][daft.DataFrame.union_by_name] | Returns the distinct union by name. |
<!-- END GENERATED TABLE -->

::: daft.DataFrame.concat
::: daft.DataFrame.except_all
::: daft.DataFrame.except_distinct
::: daft.DataFrame.intersect
::: daft.DataFrame.intersect_all
::: daft.DataFrame.join
::: daft.DataFrame.union
::: daft.DataFrame.union_all
::: daft.DataFrame.union_all_by_name
::: daft.DataFrame.union_by_name

## Materialization

These methods will materialize or execute the operations in your DataFrame and are **blocking**.

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`collect`][daft.DataFrame.collect] | Executes the entire DataFrame and materializes the results. |
| [`show`][daft.DataFrame.show] | Executes enough of the DataFrame in order to display the first ``n`` rows. |
| [`to_arrow`][daft.DataFrame.to_arrow] | Converts the current DataFrame to a [pyarrow Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html). |
| [`to_arrow_iter`][daft.DataFrame.to_arrow_iter] | Return an iterator of pyarrow recordbatches for this dataframe. |
| [`to_dask_dataframe`][daft.DataFrame.to_dask_dataframe] | Converts the current Daft DataFrame to a Dask DataFrame. |
| [`to_pandas`][daft.DataFrame.to_pandas] | Converts the current DataFrame to a [pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html). |
| [`to_pydict`][daft.DataFrame.to_pydict] | Converts the current DataFrame to a python dictionary. The dictionary contains Python lists of Python objects for each column. |
| [`to_pylist`][daft.DataFrame.to_pylist] | Converts the current Dataframe into a python list. |
| [`to_ray_dataset`][daft.DataFrame.to_ray_dataset] | Converts the current DataFrame to a [Ray Dataset](https://docs.ray.io/en/latest/data/api/dataset.html#ray.data.Dataset) which is useful for running distributed ML model training in Ray. |
| [`to_torch_iter_dataset`][daft.DataFrame.to_torch_iter_dataset] | Convert the current DataFrame into a `Torch IterableDataset <https://pytorch.org/docs/stable/data.html#torch.utils.data.IterableDataset>`__ for use with PyTorch. |
| [`to_torch_map_dataset`][daft.DataFrame.to_torch_map_dataset] | Convert the current DataFrame into a map-style [Torch Dataset](https://pytorch.org/docs/stable/data.html#map-style-datasets) for use with PyTorch. |
<!-- END GENERATED TABLE -->

::: daft.DataFrame.collect
::: daft.DataFrame.show
::: daft.DataFrame.to_arrow
::: daft.DataFrame.to_arrow_iter
::: daft.DataFrame.to_dask_dataframe
::: daft.DataFrame.to_pandas
::: daft.DataFrame.to_pydict
::: daft.DataFrame.to_pylist
::: daft.DataFrame.to_ray_dataset
::: daft.DataFrame.to_torch_iter_dataset
::: daft.DataFrame.to_torch_map_dataset

## Iteration

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`__iter__`][daft.DataFrame.__iter__] | Alias of `self.iter_rows()` with default arguments for convenient access of data. |
| [`iter_partitions`][daft.DataFrame.iter_partitions] | Begin executing this dataframe and return an iterator over the partitions. |
| [`iter_rows`][daft.DataFrame.iter_rows] | Return an iterator of rows for this dataframe. |
<!-- END GENERATED TABLE -->

::: daft.DataFrame.__iter__
::: daft.DataFrame.iter_partitions
::: daft.DataFrame.iter_rows

## Partitioning

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`into_partitions`][daft.DataFrame.into_partitions] | Splits or coalesces DataFrame to ``num`` partitions. Order is preserved. |
| [`num_partitions`][daft.DataFrame.num_partitions] |  |
| [`repartition`][daft.DataFrame.repartition] | Repartitions DataFrame to ``num`` partitions. |
<!-- END GENERATED TABLE -->

::: daft.DataFrame.into_partitions
::: daft.DataFrame.num_partitions
::: daft.DataFrame.repartition

## Visualization

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`describe`][daft.DataFrame.describe] | Returns the Schema of the DataFrame, which provides information about each column, as a new DataFrame. |
| [`explain`][daft.DataFrame.explain] | Prints the (logical and physical) plans that will be executed to produce this DataFrame. |
<!-- END GENERATED TABLE -->

::: daft.DataFrame.describe
::: daft.DataFrame.explain

## Metadata

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`__contains__`][daft.DataFrame.__contains__] | Returns whether the column exists in the dataframe. |
| [`__len__`][daft.DataFrame.__len__] | Returns the count of rows when dataframe is materialized. |
| [`schema`][daft.DataFrame.schema] | Returns the Schema of the DataFrame, which provides information about each column, as a Python object. |
<!-- END GENERATED TABLE -->

::: daft.DataFrame.__contains__
::: daft.DataFrame.__len__
::: daft.DataFrame.schema

## Utility

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`pipe`][daft.DataFrame.pipe] | Apply the function to this DataFrame. |
<!-- END GENERATED TABLE -->

::: daft.DataFrame.pipe
