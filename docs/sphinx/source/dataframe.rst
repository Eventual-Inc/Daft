DataFrame
=========

.. currentmodule:: daft

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    DataFrame

.. NOTE::
    Most DataFrame methods are **lazy**, meaning that they do not execute computation immediately when invoked. Instead, these operations are enqueued in
    the DataFrame's internal query plan, and are only executed when `Execution`_ DataFrame methods are called.

.. _dataframe-api-operations:

Data Manipulation
#################

Selecting Columns
*****************

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    DataFrame.__getitem__

Manipulating Columns
********************

.. _df-select:
.. _df-with-column:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    DataFrame.select
    DataFrame.with_column
    DataFrame.with_columns
    DataFrame.with_column_renamed
    DataFrame.with_columns_renamed
    DataFrame.pivot
    DataFrame.exclude
    DataFrame.explode
    DataFrame.unpivot
    DataFrame.melt
    DataFrame.transform

Filtering Rows
**************

.. _df-where:
.. _df-limit:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    DataFrame.distinct
    DataFrame.filter
    DataFrame.where
    DataFrame.limit
    DataFrame.sample
    DataFrame.drop_nan
    DataFrame.drop_null

Reordering
**********

.. _df-sort:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    DataFrame.sort
    DataFrame.repartition
    DataFrame.into_partitions

Combining
*********

.. _df-join:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    DataFrame.join
    DataFrame.concat
    DataFrame.union
    DataFrame.union_all
    DataFrame.union_by_name
    DataFrame.union_all_by_name
    DataFrame.intersect
    DataFrame.intersect_all
    DataFrame.except_distinct
    DataFrame.except_all

.. _df-aggregations:

Aggregations
************

.. _df-groupby:
.. _df-sum:
.. _df-mean:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    DataFrame.agg
    DataFrame.groupby
    DataFrame.sum
    DataFrame.mean
    DataFrame.stddev
    DataFrame.count
    DataFrame.min
    DataFrame.max
    DataFrame.any_value
    DataFrame.agg_list
    DataFrame.agg_set
    DataFrame.agg_concat

Execution
#########

.. NOTE::
    These methods will execute the operations in your DataFrame and **are blocking**.

Data Retrieval
**************

These methods will run the dataframe and retrieve them to where the code is being run.

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    DataFrame.to_pydict
    DataFrame.to_pylist
    DataFrame.iter_partitions
    DataFrame.iter_rows
    DataFrame.__iter__
    DataFrame.to_arrow_iter

Materialization
***************

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    DataFrame.collect

Visualization
*************

.. _df-show:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    DataFrame.show


.. _df-write-data:

Writing Data
************

.. _df-writing-data:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    DataFrame.write_parquet
    DataFrame.write_csv
    DataFrame.write_iceberg
    DataFrame.write_deltalake
    DataFrame.write_lance

Integrations
************

.. _df-to-integrations:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    DataFrame.to_arrow
    DataFrame.to_pandas
    DataFrame.to_torch_map_dataset
    DataFrame.to_torch_iter_dataset
    DataFrame.to_ray_dataset
    DataFrame.to_dask_dataframe

Schema and Lineage
##################

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    DataFrame.explain
    DataFrame.schema
    DataFrame.describe
    DataFrame.column_names
    DataFrame.columns
    DataFrame.__contains__

Statistics
##########

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    DataFrame.summarize
    DataFrame.count_rows
    DataFrame.__len__
