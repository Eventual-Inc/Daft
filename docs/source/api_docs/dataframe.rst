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
    DataFrame.where
    DataFrame.limit
    DataFrame.sample

Reordering
**********

.. _df-sort:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    DataFrame.sort
    DataFrame.repartition

Combining
*********

.. _df-join:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    DataFrame.join
    DataFrame.concat

.. _df-aggregations:

Aggregations
************

.. _df-groupby:
.. _df-sum:
.. _df-mean:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    DataFrame.groupby
    DataFrame.sum
    DataFrame.mean
    DataFrame.count
    DataFrame.min
    DataFrame.max
    DataFrame.agg

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
    DataFrame.iter_partitions

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
    DataFrame.column_names
