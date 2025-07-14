# Aggregations

When performing aggregations such as sum, mean and count, Daft enables you to group data by certain keys and aggregate within those keys. Learn more about [Aggregations and Grouping](../core_concepts.md#aggregations-and-grouping) in Daft User Guide.

<div class="grid cards" markdown>

* [**DataFrame Aggregations**](#dataframe-aggregations)

    Compute summary statistics and aggregations across the entire DataFrame or create grouped views for further processing.

* [**Grouped Aggregations**](#grouped-aggregations)

    Apply aggregation functions within each group after grouping by specified keys, returning aggregated results per group.

</div>

## DataFrame Aggregations

<!-- BEGIN GENERATED TABLE -->
| Method | Description |
|--------|-------------|
| [`agg`][daft.DataFrame.agg] | Perform aggregations on this DataFrame. |
| [`agg_concat`][daft.DataFrame.agg_concat] | Performs a global list concatenation agg on the DataFrame. |
| [`agg_list`][daft.DataFrame.agg_list] | Performs a global list agg on the DataFrame. |
| [`agg_set`][daft.DataFrame.agg_set] | Performs a global set agg on the DataFrame (ignoring nulls). |
| [`any_value`][daft.DataFrame.any_value] | Returns an arbitrary value on this DataFrame. |
| [`count`][daft.DataFrame.count] | Performs a global count on the DataFrame. |
| [`count`][daft.DataFrame.count] | Performs a global count on the DataFrame. |
| [`count_rows`][daft.DataFrame.count_rows] | Executes the Dataframe to count the number of rows. |
| [`groupby`][daft.DataFrame.groupby] | Performs a GroupBy on the DataFrame for aggregation. |
| [`max`][daft.DataFrame.max] | Performs a global max on the DataFrame. |
| [`mean`][daft.DataFrame.mean] | Performs a global mean on the DataFrame. |
| [`min`][daft.DataFrame.min] | Performs a global min on the DataFrame. |
| [`stddev`][daft.DataFrame.stddev] | Performs a global standard deviation on the DataFrame. |
| [`sum`][daft.DataFrame.sum] | Performs a global sum on the DataFrame. |
| [`summarize`][daft.DataFrame.summarize] | Returns column statistics for the DataFrame. |
<!-- END GENERATED TABLE -->

::: daft.DataFrame.agg
::: daft.DataFrame.agg_concat
::: daft.DataFrame.agg_list
::: daft.DataFrame.agg_set
::: daft.DataFrame.any_value
::: daft.DataFrame.count
::: daft.DataFrame.count
::: daft.DataFrame.count_rows
::: daft.DataFrame.groupby
::: daft.DataFrame.max
::: daft.DataFrame.mean
::: daft.DataFrame.min
::: daft.DataFrame.stddev
::: daft.DataFrame.sum
::: daft.DataFrame.summarize

## Grouped Aggregations

Calling [`df.groupby()`][daft.DataFrame.groupby] returns a `GroupedDataFrame` object which is a view of the original DataFrame but with additional context on which keys to group on. You can then call various aggregation methods to run the aggregation within each group, returning a new DataFrame.

::: daft.dataframe.dataframe.GroupedDataFrame
    options:
        filters: ["!^_"]
