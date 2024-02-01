Apache Iceberg
==============

`Apache Iceberg <https://iceberg.apache.org/>`_ is an open-sourced table format originally developed at Netflix for large-scale analytical datasets.

To read from the Apache Iceberg table format, use the :func:`daft.read_iceberg` function.

We integrate closely with `PyIceberg <https://py.iceberg.apache.org/>`_ (the official Python implementation for Apache Iceberg) and allow the reading of Daft dataframes easily from PyIceberg's Table objects.

.. code:: python

    # Access a PyIceberg table as per normal
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog("my_iceberg_catalog")
    table = catalog.load_table("my_namespace.my_table")

    # Create a Daft Dataframe
    import daft

    df = daft.read_iceberg(table)

Daft currently natively supports:

1. **Distributed Reads:** Daft will fully distribute the I/O of reads over your compute resources (whether Ray or on multithreading on the local PyRunner)
2. **Skipping filtered data:** Daft uses ``df.where(...)`` filter calls to only read data that matches your predicates
3. **All Catalogs From PyIceberg:** Daft is natively integrated with PyIceberg, and supports all the catalogs that PyIceberg does!

Selecting a Table
*****************

Daft currently leverages PyIceberg for catalog/table discovery. Please consult `PyIceberg documentation <https://py.iceberg.apache.org/api/#load-a-table>`_ for more details on how to load a table!

Roadmap
*******

Here are features of Iceberg that are works-in-progress.

1. Iceberg V2 merge-on-read features
2. Writing back to an Iceberg table (appends, overwrites, upserts)
