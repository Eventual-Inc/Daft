# DataFrame

Most DataFrame methods are **lazy**, meaning that they do not execute computation immediately when invoked. Instead, these operations are enqueued in the DataFrame's internal query plan, and are only executed when Execution DataFrame methods are called. Learn more about [DataFrames](../core_concepts.md#dataframe) in Daft User Guide.

<!-- ::: daft.DataFrame
    options:
        filters: ["!^_[^_]", "!__repr__", "!__column_input_to_expression", "!__builder"] -->

## DataFrame Creation

## Data Manipulation

### Selecting Columns

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`__getitem__`][daft.DataFrame.__getitem__] | Gets a column from the DataFrame as an Expression (``df["mycol"]``). |
<!-- END GENERATED TABLE -->

::: daft.DataFrame.__getitem__

### Manipulating Columns

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`exclude`][daft.DataFrame.exclude] | Drops columns from the current DataFrame by name. |
| [`explode`][daft.DataFrame.explode] | Explodes a List column, where every element in each row's List becomes its own row, and all other columns in the DataFrame are duplicated across rows. |
| [`melt`][daft.DataFrame.melt] | Alias for unpivot. |
| [`pivot`][daft.DataFrame.pivot] | Pivots a column of the DataFrame and performs an aggregation on the values. |
| [`select`][daft.DataFrame.select] | Creates a new DataFrame from the provided expressions, similar to a SQL ``SELECT``. |
| [`unpivot`][daft.DataFrame.unpivot] | Unpivots a DataFrame from wide to long format. |
| [`with_column`][daft.DataFrame.with_column] | Adds a column to the current DataFrame with an Expression, equivalent to a ``select`` with all current columns and the new one. |
| [`with_columns`][daft.DataFrame.with_columns] | Adds columns to the current DataFrame with Expressions, equivalent to a ``select`` with all current columns and the new ones. |
<!-- END GENERATED TABLE -->

::: daft.DataFrame.exclude
::: daft.DataFrame.explode
::: daft.DataFrame.melt
::: daft.DataFrame.pivot
::: daft.DataFrame.select
::: daft.DataFrame.unpivot
::: daft.DataFrame.with_column
::: daft.DataFrame.with_columns
