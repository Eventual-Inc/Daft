# DataFrame

<!-- ::: daft.DataFrame -->

::: daft.DataFrame.select
    options:
        summary: true
        show_docstring_examples: false
        show_docstring_parameters: false
        show_docstring_returns: false

<!-- ::: daft.dataframe
    options:
      members: true
      show_root_heading: true
      show_root_full_path: false
      show_object_full_path: false
      show_category_heading: true
      show_if_no_docstring: true
      show_source: true -->

<!-- # DataFrame

Most DataFrame methods are **lazy**, meaning that they do not execute computation immediately when invoked. Instead, these operations are enqueued in the DataFrame's internal query plan, and are only executed when Execution DataFrame methods are called.

## Data Manipulation

### Selecting Columns

::: daft.dataframe.__getitem__
    options:
      members: true
      show_root_heading: true
      show_root_full_path: false
      show_object_full_path: false
      show_category_heading: true
      show_if_no_docstring: true
      show_source: true

### Manipulating Columns

::: daft.dataframe.select
::: daft.dataframe.with_column
::: daft.dataframe.with_columns
::: daft.dataframe.with_column_renamed
::: daft.dataframe.with_columns_renamed
::: daft.dataframe.pivot
::: daft.dataframe.exclude
::: daft.dataframe.explode
::: daft.dataframe.unpivot
::: daft.dataframe.melt
::: daft.dataframe.transform

### Filtering Rows

::: daft.dataframe.distinct
::: daft.dataframe.filter
::: daft.dataframe.where
::: daft.dataframe.limit
::: daft.dataframe.sample
::: daft.dataframe.drop_nan
::: daft.dataframe.drop_null

### Reordering

::: daft.dataframe.sort
::: daft.dataframe.repartition
::: daft.dataframe.into_partitions

### Combining

::: daft.dataframe.join
::: daft.dataframe.concat
::: daft.dataframe.union
::: daft.dataframe.union_all
::: daft.dataframe.union_by_name
::: daft.dataframe.union_all_by_name
::: daft.dataframe.intersect
::: daft.dataframe.intersect_all
::: daft.dataframe.except_distinct
::: daft.dataframe.except_all

### Aggregations

::: daft.dataframe.agg
::: daft.dataframe.groupby
::: daft.dataframe.sum
::: daft.dataframe.mean
::: daft.dataframe.stddev
::: daft.dataframe.count
::: daft.dataframe.min
::: daft.dataframe.max
::: daft.dataframe.any_value
::: daft.dataframe.agg_list
::: daft.dataframe.agg_set
::: daft.dataframe.agg_concat

## Execution

!!! note
    These methods will execute the operations in your DataFrame and **are blocking**.

### Data Retrieval

These methods will run the dataframe and retrieve them to where the code is being run.

::: daft.dataframe.to_pydict
::: daft.dataframe.to_pylist
::: daft.dataframe.iter_partitions
::: daft.dataframe.iter_rows
::: daft.dataframe.__iter__
::: daft.dataframe.to_arrow_iter

### Materialization

::: daft.dataframe.collect

### Visualization

::: daft.dataframe.show

### Writing Data

::: daft.dataframe.write_parquet
::: daft.dataframe.write_csv
::: daft.dataframe.write_iceberg
::: daft.dataframe.write_deltalake
::: daft.dataframe.write_lance

### Integrations

::: daft.dataframe.to_arrow
::: daft.dataframe.to_pandas
::: daft.dataframe.to_torch_map_dataset
::: daft.dataframe.to_torch_iter_dataset
::: daft.dataframe.to_ray_dataset
::: daft.dataframe.to_dask_dataframe

## Schema and Lineage

::: daft.dataframe.explain
::: daft.dataframe.schema
::: daft.dataframe.describe
::: daft.dataframe.column_names
::: daft.dataframe.columns
::: daft.dataframe.__contains__

## Statistics

::: daft.dataframe.summarize
::: daft.dataframe.count_rows
::: daft.dataframe.__len__ -->