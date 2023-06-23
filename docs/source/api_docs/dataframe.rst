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

Manipulating Columns
********************

.. _df-select:
.. _df-with-column:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    daft.DataFrame.select
    daft.DataFrame.with_column
    daft.DataFrame.exclude
    daft.DataFrame.explode

Filtering Rows
**************

.. _df-where:
.. _df-limit:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    daft.DataFrame.distinct
    daft.DataFrame.where
    daft.DataFrame.limit

Reordering
**********

.. _df-sort:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    daft.DataFrame.sort
    daft.DataFrame.repartition

Combining
*********

.. _df-join:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    daft.DataFrame.join
    daft.DataFrame.concat

.. _df-aggregations:

Aggregations
************

.. _df-groupby:
.. _df-sum:
.. _df-mean:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    daft.DataFrame.groupby
    daft.DataFrame.sum
    daft.DataFrame.mean
    daft.DataFrame.count
    daft.DataFrame.min
    daft.DataFrame.max
    daft.DataFrame.agg

Execution
#########

.. NOTE::
    These methods will execute the operations in your DataFrame and **are blocking**.

Materialization
***************

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    daft.DataFrame.collect

Visualization
*************

.. _df-show:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    daft.DataFrame.show


.. _df-write-data:

Writing Data
************

.. _df-writing-data:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    daft.DataFrame.write_parquet
    daft.DataFrame.write_csv

Integrations
************

.. _df-to-integrations:

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    daft.DataFrame.to_pandas
    daft.DataFrame.to_torch_map_dataset
    daft.DataFrame.to_torch_iter_dataset
    daft.DataFrame.to_ray_dataset
    daft.DataFrame.to_dask_dataframe

Schema and Lineage
##################

.. autosummary::
    :nosignatures:
    :toctree: doc_gen/dataframe_methods

    daft.DataFrame.explain
    daft.DataFrame.schema
    daft.DataFrame.column_names
