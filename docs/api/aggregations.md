# Aggregations

When performing aggregations such as sum, mean and count, Daft enables you to group data by certain keys and aggregate within those keys. Learn more about [Aggregations and Grouping](../core_concepts.md#aggregations-and-grouping) in Daft User Guide.

<div class="grid cards" markdown>

* [**DataFrame Aggregations**](#dataframe-aggregations)

    Compute summary statistics and aggregations across the entire DataFrame or create grouped views for further processing.

* [**Grouped Aggregations**](#grouped-aggregations)

    Apply aggregation functions within each group after grouping by specified keys, returning aggregated results per group.

</div>

## DataFrame Aggregations

::: daft.DataFrame.groupby
::: daft.DataFrame.sum
::: daft.DataFrame.mean
::: daft.DataFrame.stddev
::: daft.DataFrame.count
::: daft.DataFrame.min
::: daft.DataFrame.max
::: daft.DataFrame.agg
::: daft.DataFrame.agg_concat
::: daft.DataFrame.agg_list
::: daft.DataFrame.agg_set
::: daft.DataFrame.any_value
::: daft.DataFrame.count
::: daft.DataFrame.count_rows
::: daft.DataFrame.summarize

## Grouped Aggregations

Calling [`df.groupby()`][daft.DataFrame.groupby] returns a `GroupedDataFrame` object which is a view of the original DataFrame but with additional context on which keys to group on. You can then call various aggregation methods to run the aggregation within each group, returning a new DataFrame.

::: daft.dataframe.dataframe.GroupedDataFrame
    options:
        filters: ["!^_"]
