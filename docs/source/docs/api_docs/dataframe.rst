DataFrame
=========

.. autoclass:: daft.DataFrame

Construction
############

.. _df-file-construction-api:

.. automethod:: daft.DataFrame.read_csv
.. automethod:: daft.DataFrame.read_json
.. automethod:: daft.DataFrame.read_parquet
.. automethod:: daft.DataFrame.from_files

.. _df-memory-construction-api:

.. automethod:: daft.DataFrame.from_pylist
.. automethod:: daft.DataFrame.from_pydict

.. _dataframe-api-operations:

Operations
##########

Manipulating Columns
********************
.. _df-select:
.. automethod:: daft.DataFrame.select
.. _df-with-column:
.. automethod:: daft.DataFrame.with_column
.. automethod:: daft.DataFrame.exclude
.. automethod:: daft.DataFrame.explode

Filtering Rows
**************
.. automethod:: daft.DataFrame.distinct
.. _df-where:
.. automethod:: daft.DataFrame.where
.. _df-limit:
.. automethod:: daft.DataFrame.limit

Reordering
**********
.. _df-sort:
.. automethod:: daft.DataFrame.sort
.. automethod:: daft.DataFrame.repartition

Combining
*********
.. _df-join:
.. automethod:: daft.DataFrame.join

Aggregations
************
.. _df-groupby:
.. automethod:: daft.DataFrame.groupby
.. _df-sum:
.. automethod:: daft.DataFrame.sum
.. _df-mean:
.. automethod:: daft.DataFrame.mean

Execution
#########

Visualization
*************

.. _df-show:
.. automethod:: daft.DataFrame.show
.. _df-to-pandas:
.. automethod:: daft.DataFrame.to_pandas

.. _df-write-data:

Writing Data
************

.. automethod:: daft.DataFrame.write_parquet
.. automethod:: daft.DataFrame.write_csv

Integrations
************

.. automethod:: daft.DataFrame.to_ray_dataset

Schema and Lineage
##################

.. automethod:: daft.DataFrame.explain
.. automethod:: daft.DataFrame.schema
.. autoproperty:: daft.DataFrame.column_names
