Unity Catalog
=============

`Unity Catalog <https://github.com/unitycatalog/unitycatalog//>`_ is an open-sourced catalog developed by Databricks.
Users of Unity Catalog are able to work with data assets such as tables (Parquet, CSV, Iceberg, Delta), volumes
(storing raw files), functions and models.

To use Daft with the Unity Catalog, you will need to install Daft with the `unity` option specified like so:

```
pip install getdaft[unity]
```

.. WARNING::

    These APIs are in beta and may be subject to change as the Unity Catalog continues to be developed.

Connecting to the Unity Catalog
*******************************

Daft includes an abstraction for the Unity Catalog.

.. code:: python

    from daft.unity_catalog import UnityCatalog

    unity = UnityCatalog(
        endpoint="https://<databricks_workspace_id>.cloud.databricks.com",
        # Authentication can be retrieved from your provider of Unity Catalog
        token="my-token",
    )

    # See all available catalogs
    print(unity.list_catalogs())

    # See available schemas in a given catalog
    print(unity.list_schemas("my_catalog_name"))

    # See available tables in a given schema
    print(unity.list_tables("my_catalog_name.my_schema_name"))

Loading a Daft Dataframe from a Delta Lake table in Unity Catalog
*****************************************************************

.. code:: python

    unity_table = unity.load_table("my_catalog_name.my_schema_name.my_table_name")

    df = daft.read_delta_lake(unity_table)
    df.show()

Any subsequent filter operations on the Daft ``df`` DataFrame object will be correctly optimized to take advantage of DeltaLake features

.. code:: python

    # Filter which takes advantage of partition pruning capabilities of Delta Lake
    df = df.where(df["partition_key"] < 1000)
    df.show()

See also :doc:`delta_lake` for more information about how to work with the Delta Lake tables provided by the Unity Catalog.

Roadmap
*******

1. Volumes integration for reading objects from volumes (e.g. images and documents)
2. Unity Iceberg integration for reading tables using the Iceberg interface instead of the Delta Lake interface

Please make issues on the Daft repository if you have any use-cases that Daft does not currently cover!
