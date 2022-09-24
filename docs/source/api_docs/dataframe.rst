DataFrame
=========

.. autoclass:: daft.DataFrame

Construction
############

.. automethod:: daft.DataFrame.from_pylist
.. automethod:: daft.DataFrame.from_pydict
.. automethod:: daft.DataFrame.from_csv
.. automethod:: daft.DataFrame.from_json
.. automethod:: daft.DataFrame.from_parquet

Operations
##########

Manipulating Columns
********************
.. automethod:: daft.DataFrame.select
.. automethod:: daft.DataFrame.with_column
.. automethod:: daft.DataFrame.exclude

Filtering Rows
**************
.. automethod:: daft.DataFrame.distinct
.. automethod:: daft.DataFrame.where
.. automethod:: daft.DataFrame.limit

Reordering
**********
.. automethod:: daft.DataFrame.sort
.. automethod:: daft.DataFrame.repartition

Combining
*********
.. automethod:: daft.DataFrame.join

Aggregations
************
.. automethod:: daft.DataFrame.groupby
.. automethod:: daft.DataFrame.sum
.. automethod:: daft.DataFrame.mean

Execution
#########

.. automethod:: daft.DataFrame.collect
.. automethod:: daft.DataFrame.show
.. automethod:: daft.DataFrame.to_pandas

Schema and Lineage
##################

.. automethod:: daft.DataFrame.plan
.. automethod:: daft.DataFrame.schema
.. automethod:: daft.DataFrame.column_names
