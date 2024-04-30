Apache Hudi
===========

`Apache Hudi <https://hudi.apache.org/>`__ is an open-sourced transactional data lake platform that brings database and data warehouse capabilities to data lakes. Hudi supports transactions, efficient upserts/deletes, advanced indexes, streaming ingestion services, data clustering/compaction optimizations, and concurrency all while keeping your data in open source file formats.

Daft currently supports:

1. **Parallel + Distributed Reads:** Daft parallelizes Hudi table reads over all cores of your machine, if using the default multithreading runner, or all cores + machines of your Ray cluster, if using the :ref:`distributed Ray runner <scaling_up>`.
2. **Skipping Filtered Data:** Daft ensures that only data that matches your :meth:`df.where(...) <daft.DataFrame.where>` filter will be read, often skipping entire files/partitions.
3. **Multi-cloud Support:** Daft supports reading Hudi tables from AWS S3, Azure Blob Store, and GCS, as well as local files.

Installing Daft with Apache Hudi Support
****************************************

Daft supports installing Hudi through optional dependency.

.. code-block:: shell

    pip install -U "getdaft[hudi]"

Reading a Table
***************

To read from an Apache Hudi table, use the :func:`daft.read_hudi` function. The following is an example snippet of loading an example table

.. code:: python

    # Read Apache Hudi table into a Daft DataFrame.
    import daft

    df = daft.read_hudi("some-table-uri")
    df = df.where(df["foo"] > 5)
    df.show()

Type System
***********

Daft and Hudi have compatible type systems. Here are how types are converted across the two systems.

When reading from a Hudi table into Daft:

+-----------------------------+------------------------------------------------------------------------------------------+
| Apache Hudi                 | Daft                                                                                     |
+=============================+==========================================================================================+
| **Primitive Types**                                                                                                    |
+-----------------------------+------------------------------------------------------------------------------------------+
| `boolean`                   | :meth:`daft.DataType.bool() <daft.DataType.bool>`                                        |
+-----------------------------+------------------------------------------------------------------------------------------+
| `byte`                      | :meth:`daft.DataType.int8() <daft.DataType.int8>`                                        |
+-----------------------------+------------------------------------------------------------------------------------------+
| `short`                     | :meth:`daft.DataType.int16() <daft.DataType.int16>`                                      |
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
| `timestamp`                 | :meth:`daft.DataType.timestamp(timeunit="us", timezone=None) <daft.DataType.timestamp>`  |
+-----------------------------+------------------------------------------------------------------------------------------+
| `timestampz`                | :meth:`daft.DataType.timestamp(timeunit="us", timezone="UTC") <daft.DataType.timestamp>` |
+-----------------------------+------------------------------------------------------------------------------------------+
| `string`                    | :meth:`daft.DataType.string() <daft.DataType.string>`                                    |
+-----------------------------+------------------------------------------------------------------------------------------+
| `binary`                    | :meth:`daft.DataType.binary() <daft.DataType.binary>`                                    |
+-----------------------------+------------------------------------------------------------------------------------------+
| **Nested Types**                                                                                                       |
+-----------------------------+------------------------------------------------------------------------------------------+
| `struct(fields)`            | :meth:`daft.DataType.struct(fields) <daft.DataType.struct>`                              |
+-----------------------------+------------------------------------------------------------------------------------------+
| `list(child_type)`          | :meth:`daft.DataType.list(child_type) <daft.DataType.list>`                              |
+-----------------------------+------------------------------------------------------------------------------------------+
| `map(key_type, value_type)` | :meth:`daft.DataType.map(key_type, value_type) <daft.DataType.map>`                      |
+-----------------------------+------------------------------------------------------------------------------------------+

Roadmap
*******

Currently there are limitations of reading Hudi tables

- Only support snapshot read of Copy-on-Write tables
- Only support reading table version 5 & 6 (tables created using release 0.12.x - 0.15.x)
- Table must not have ``hoodie.datasource.write.drop.partition.columns=true``

Support for more Hudi features are tracked as below:

1. Support incremental query for Copy-on-Write tables (`issue <https://github.com/Eventual-Inc/Daft/issues/2153>`__).
2. Read support for 1.0 table format (`issue <https://github.com/Eventual-Inc/Daft/issues/2152>`__).
3. Read support (snapshot) for Merge-on-Read tables (`issue <https://github.com/Eventual-Inc/Daft/issues/2154>`__).
4. Write support (`issue <https://github.com/Eventual-Inc/Daft/issues/2155>`__).
