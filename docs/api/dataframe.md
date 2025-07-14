# DataFrame

Most DataFrame methods are **lazy**, meaning that they do not execute computation immediately when invoked. Instead, these operations are enqueued in the DataFrame's internal query plan, and are only executed when Execution DataFrame methods are called.

Learn more about [DataFrames](../core_concepts.md#dataframe) in Daft User Guide and see also [Aggregations API Reference](aggregations.md).

<!-- ::: daft.DataFrame
    options:
        filters: ["!^_[^_]", "!__repr__", "!__column_input_to_expression", "!__builder"] -->

## Selecting Columns

::: daft.DataFrame.__getitem__

## Manipulating Columns

::: daft.DataFrame.exclude
::: daft.DataFrame.explode
::: daft.DataFrame.melt
::: daft.DataFrame.pivot
::: daft.DataFrame.select
::: daft.DataFrame.unpivot
::: daft.DataFrame.with_column
::: daft.DataFrame.with_column_renamed
::: daft.DataFrame.with_columns
::: daft.DataFrame.with_columns_renamed

## Filtering Rows

::: daft.DataFrame.distinct
::: daft.DataFrame.filter
::: daft.DataFrame.limit
::: daft.DataFrame.sample
::: daft.DataFrame.where

## Reordering

::: daft.DataFrame.into_partitions
::: daft.DataFrame.repartition
::: daft.DataFrame.sort

## Combining

::: daft.DataFrame.concat
::: daft.DataFrame.join

## Execution

These methods will execute the operations in your DataFrame and are **blocking**.

::: daft.DataFrame.collect
::: daft.DataFrame.show

## Converting

::: daft.DataFrame.to_arrow
::: daft.DataFrame.to_arrow_iter
::: daft.DataFrame.to_dask_dataframe
::: daft.DataFrame.to_pandas
::: daft.DataFrame.to_pydict
::: daft.DataFrame.to_pylist
::: daft.DataFrame.to_ray_dataset
::: daft.DataFrame.to_torch_iter_dataset
::: daft.DataFrame.to_torch_map_dataset

## Data Retrieval

::: daft.DataFrame.__iter__
::: daft.DataFrame.iter_partitions
::: daft.DataFrame.iter_rows
::: daft.DataFrame.to_pydict
::: daft.DataFrame.to_pylist

## Visualization

::: daft.DataFrame.explain
