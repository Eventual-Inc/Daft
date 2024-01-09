Data Catalogs
=============

**Data Catalogs** are services that provide access to **Tables** of data. **Tables** are powerful abstractions for large datasets in storage, providing many benefits over naively storing data as just a bunch of CSV/Parquet files.

There are many different **Table Formats** that are employed by Data Catalogs. These table formats will differ implementation and capabilities, but will often provide advantages such as:

1. **Schema:** what data do these files contain?
2. **Partitioning Specification:** how is the data organized?
3. **Statistics/Metadata:** how many rows does each file contain, and what are the min/max values of each files' columns?
4. **ACID compliance:** updates to the table are atomic

.. NOTE::
    The names of Table Formats and their Data Catalogs are often used interchangeably.

    For example, "Apache Iceberg" often refers to both the Data Catalog and its Table Format.

    You can retrieve an **Apache Iceberg Table** from an **Apache Iceberg REST Data Catalog**.

    However, some Data Catalogs allow for many different underlying Table Formats. For example, you can request both an **Apache Iceberg Table** or a **Hive Table** from an **AWS Glue Data Catalog**.

Why use Data Catalogs?
----------------------

Daft can effectively leverage the statistics and metadata provided by these Data Catalogs' Tables to dramatically speed up queries.

This is accomplished by techniques such as:

1. **Partition pruning:** ignore files where their partition values don't match filter predicates
2. **Schema retrieval:** convert the schema provided by the data catalog into a Daft schema instead of sampling a schema from the data
3. **Metadata execution**: utilize metadata such as row counts to read the bare minimum amount of data necessary from storage

Data Catalog Integrations
-------------------------

Apache Iceberg
^^^^^^^^^^^^^^

Apache Iceberg is an open-sourced table format originally developed at Netflix for large-scale analytical datasets.

To read from the Apache Iceberg table format, use the :func:`daft.read_iceberg` function.

We integrate closely with `PyIceberg <https://py.iceberg.apache.org/>`_ (the official Python implementation for Apache Iceberg) and allow the reading of Daft dataframes from PyIceberg's Table objects.
