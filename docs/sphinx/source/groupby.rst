GroupBy
=======

When performing aggregations such as sum, mean and count, you may often want to group data by certain keys and aggregate within those keys.

Calling :meth:`df.groupby() <daft.DataFrame.groupby>` returns a ``GroupedDataFrame`` object which is a view of the original DataFrame but with additional context on which keys to group on. You can then call various aggregation methods to run the aggregation within each group, returning a new DataFrame.

.. autoclass:: daft.dataframe.GroupedDataFrame
    :members:
