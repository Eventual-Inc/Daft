# DataFrame

Most DataFrame methods are **lazy**, meaning that they do not execute computation immediately when invoked. Instead, these operations are enqueued in the DataFrame's internal query plan, and are only executed when Execution DataFrame methods are called.

Learn more about [DataFrames](../core_concepts.md#dataframe) in Daft User Guide and see also [Aggregations API Reference](aggregations.md).

<div class="grid cards api" markdown>

* [**Selecting Columns**](#selecting-columns)

    Choose specific columns to include or exclude from the DataFrame.

* [**Filtering Rows**](#filtering-rows)

    Remove rows based on conditions, duplicates, null values, or sample subsets.

* [**Manipuating Columns**](#manipulating-columns)

    Add, rename, transform, and reshape columns including pivoting and exploding operations.

* [**Reordering**](#reordering)

    Sort DataFrame rows by column values and limit the number of returned rows.

* [**Combining**](#combining)

    Merge multiple DataFrames through joins, unions, intersections, and set operations.

* [**Materialization**](#materialization)

    Execute lazy operations and convert DataFrames to concrete formats like Arrow, Pandas, or PyTorch datasets.

* [**Iteration**](#iteration)

    Iterate through DataFrame contents lazily without full materialization, processing data row-by-row or partition-by-partition.

* [**Partitioning**](#partitioning)

    Control how data is distributed across partitions for parallel processing optimization.

* [**Visualization**](#visualization)

    Display DataFrame contents and query execution plans for debugging and inspection.

* [**Metadata**](#metadata)

    Access DataFrame schema, length, and column membership information without triggering computation.

* [**Utility**](#utility)

    Apply custom functions to the DataFrame using functional programming patterns.

</div>

## Selecting Columns

::: daft.DataFrame.select
::: daft.DataFrame.exclude

## Filtering Rows

::: daft.DataFrame.distinct
::: daft.DataFrame.filter
::: daft.DataFrame.limit
::: daft.DataFrame.sample
::: daft.DataFrame.where
::: daft.DataFrame.drop_duplicates
::: daft.DataFrame.drop_nan
::: daft.DataFrame.drop_null
::: daft.DataFrame.unique

## Manipulating Columns

::: daft.DataFrame.explode
::: daft.DataFrame.melt
::: daft.DataFrame.pivot
::: daft.DataFrame.unpivot
::: daft.DataFrame.with_column
::: daft.DataFrame.with_column_renamed
::: daft.DataFrame.with_columns
::: daft.DataFrame.with_columns_renamed
::: daft.DataFrame.transform

## Reordering

::: daft.DataFrame.sort
::: daft.DataFrame.limit

## Combining

::: daft.DataFrame.concat
::: daft.DataFrame.join
::: daft.DataFrame.union
::: daft.DataFrame.union_all
::: daft.DataFrame.union_all_by_name
::: daft.DataFrame.union_by_name
::: daft.DataFrame.intersect
::: daft.DataFrame.intersect_all
::: daft.DataFrame.except_all
::: daft.DataFrame.except_distinct

## Materialization

These methods will materialize or execute the operations in your DataFrame and are **blocking**.

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

::: daft.DataFrame.__iter__
::: daft.DataFrame.iter_partitions
::: daft.DataFrame.iter_rows

## Partitioning

::: daft.DataFrame.into_partitions
::: daft.DataFrame.num_partitions
::: daft.DataFrame.repartition

## Visualization

::: daft.DataFrame.explain
::: daft.DataFrame.describe

## Metadata

::: daft.DataFrame.__contains__
::: daft.DataFrame.__len__
::: daft.DataFrame.schema

## Utility

::: daft.DataFrame.pipe
