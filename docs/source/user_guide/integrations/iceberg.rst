Apache Iceberg
==============

`Apache Iceberg <https://iceberg.apache.org/>`_ is an open-sourced table format originally developed at Netflix for large-scale analytical datasets.

Daft currently natively supports:

1. **Distributed Reads:** Daft will fully distribute the I/O of reads over your compute resources (whether Ray or on multithreading on the local PyRunner)
2. **Skipping Filtered Data:** Daft uses ``df.where(...)`` filter calls to only read data that matches your predicates
3. **All Catalogs From PyIceberg:** Daft is natively integrated with PyIceberg, and supports all the catalogs that PyIceberg does

Reading a Table
***************

To read from the Apache Iceberg table format, use the :func:`daft.read_iceberg` function.

We integrate closely with `PyIceberg <https://py.iceberg.apache.org/>`_ (the official Python implementation for Apache Iceberg) and allow the reading of Daft dataframes easily from PyIceberg's Table objects.
The following is an example snippet of loading an example table, but for more information please consult the `PyIceberg Table loading documentation <https://py.iceberg.apache.org/api/#load-a-table>`_.

.. code:: python

    # Access a PyIceberg table as per normal
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog("my_iceberg_catalog")
    table = catalog.load_table("my_namespace.my_table")

After a table is loaded as the ``table`` object, reading it into a DataFrame is extremely easy.

.. code:: python

    # Create a Daft Dataframe
    import daft

    df = daft.read_iceberg(table)

Any subsequent filter operations on the Daft ``df`` DataFrame object will be correctly optimized to take advantage of Iceberg features such as hidden partitioning and file-level statistics for efficient reads.

.. code:: python

    # Filter which takes advantage of partition pruning capabilities of Iceberg
    df = df.where(df["partition_key"] < 1000)
    df.show()

Writing to a Table
******************

To write to an Apache Iceberg table, use the :meth:`daft.DataFrame.write_iceberg` method.

The following is an example of appending data to an Iceberg table:

.. code:: python

    written_df = df.write_iceberg(table, mode="append")
    written_df.show()

This call will then return a DataFrame containing the operations that were performed on the Iceberg table, like so:

.. code::

    ╭───────────┬───────┬───────────┬────────────────────────────────╮
    │ operation ┆ rows  ┆ file_size ┆ file_name                      │
    │ ---       ┆ ---   ┆ ---       ┆ ---                            │
    │ Utf8      ┆ Int64 ┆ Int64     ┆ Utf8                           │
    ╞═══════════╪═══════╪═══════════╪════════════════════════════════╡
    │ ADD       ┆ 5     ┆ 707       ┆ 2f1a2bb1-3e64-49da-accd-1074e… │
    ╰───────────┴───────┴───────────┴────────────────────────────────╯

Type System
***********

Daft and Iceberg have compatible type systems. Here are how types are converted across the two systems.

When reading from an Iceberg table into Daft:

+-----------------------------+------------------------------------------------------------------------------------------+
| Iceberg                     | Daft                                                                                     |
+=============================+==========================================================================================+
| **Primitive Types**                                                                                                    |
+-----------------------------+------------------------------------------------------------------------------------------+
| `boolean`                   | :meth:`daft.DataType.bool() <daft.DataType.bool>`                                        |
+-----------------------------+------------------------------------------------------------------------------------------+
| `int`                       | :meth:`daft.DataType.int32() <daft.DataType.int32>`                                      |
+-----------------------------+------------------------------------------------------------------------------------------+
| `long`                      | :meth:`daft.DataType.int64() <daft.DataType.int64>`                                      |
+-----------------------------+------------------------------------------------------------------------------------------+
| `float`                     | :meth:`daft.DataType.float32() <daft.DataType.float32>`                                  |
+-----------------------------+------------------------------------------------------------------------------------------+
| `double`                    | :meth:`daft.DataType.float64() <daft.DataType.float64>`                                  |
+-----------------------------+------------------------------------------------------------------------------------------+
| `decimal(precision, scale)` | :meth:`daft.DataType.decimal128(precision, scale) <daft.DataType.decimal128>`            |
+-----------------------------+------------------------------------------------------------------------------------------+
| `date`                      | :meth:`daft.DataType.date() <daft.DataType.date>`                                        |
+-----------------------------+------------------------------------------------------------------------------------------+
| `time`                      | :meth:`daft.DataType.int64() <daft.DataType.int64>`                                      |
+-----------------------------+------------------------------------------------------------------------------------------+
| `timestamp`                 | :meth:`daft.DataType.timestamp(timeunit="us", timezone=None) <daft.DataType.timestamp>`  |
+-----------------------------+------------------------------------------------------------------------------------------+
| `timestampz`                | :meth:`daft.DataType.timestamp(timeunit="us", timezone="UTC") <daft.DataType.timestamp>` |
+-----------------------------+------------------------------------------------------------------------------------------+
| `string`                    | :meth:`daft.DataType.string() <daft.DataType.string>`                                    |
+-----------------------------+------------------------------------------------------------------------------------------+
| `uuid`                      | :meth:`daft.DataType.binary() <daft.DataType.binary>`                                    |
+-----------------------------+------------------------------------------------------------------------------------------+
| `fixed(L)`                  | :meth:`daft.DataType.binary() <daft.DataType.binary>`                                    |
+-----------------------------+------------------------------------------------------------------------------------------+
| `binary`                    | :meth:`daft.DataType.binary() <daft.DataType.binary>`                                    |
+-----------------------------+------------------------------------------------------------------------------------------+
| **Nested Types**                                                                                                       |
+-----------------------------+------------------------------------------------------------------------------------------+
| `struct(fields)`            | :meth:`daft.DataType.struct(fields) <daft.DataType.struct>`                              |
+-----------------------------+------------------------------------------------------------------------------------------+
| `list(child_type)`          | :meth:`daft.DataType.list(child_type) <daft.DataType.list>`                              |
+-----------------------------+------------------------------------------------------------------------------------------+
| `map(K, V)`                 | :meth:`daft.DataType.struct({"key": K, "value": V}) <daft.DataType.struct>`              |
+-----------------------------+------------------------------------------------------------------------------------------+

Roadmap
*******

Here are features of Iceberg that are works-in-progress.

1. Iceberg V2 merge-on-read features
2. Writing to partitioned Iceberg tables (this is currently pending functionality to be added to the PyIceberg library)
3. More extensive usage of Iceberg-provided statistics to further optimize queries
