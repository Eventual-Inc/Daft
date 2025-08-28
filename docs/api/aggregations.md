# Aggregations

When performing aggregations such as sum, mean and count, Daft enables you to group data by certain keys and aggregate within those keys.

Calling [`df.groupby()`][daft.DataFrame.groupby] returns a `GroupedDataFrame` object which is a view of the original DataFrame but with additional context on which keys to group on. You can then call various aggregation methods to run the aggregation within each group, returning a new DataFrame.


::: daft.dataframe.dataframe.GroupedDataFrame
    options:
        filters: ["!^_"]
