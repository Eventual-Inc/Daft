# Unity Catalog

[Unity Catalog](https://github.com/unitycatalog/unitycatalog/) is an open-sourced catalog developed by Databricks. Users of Unity Catalog are able to work with data assets such as tables (Parquet, CSV, Iceberg, Delta), volumes (storing raw files), functions and models.

To use Daft with the Unity Catalog, you will need to install Daft with the `unity` option specified like so:

```bash
pip install daft[unity]
```

!!! warning "Warning"

    These APIs are in beta and may be subject to change as the Unity Catalog continues to be developed.

## Connecting to the Unity Catalog

Daft includes an abstraction for the Unity Catalog. For more information, see also [Unity Catalog Documentation](https://docs.unitycatalog.io/integrations/unity-catalog-daft/).

=== "🐍 Python"

    ```python
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
    ```

## Loading a Dataframe from a Delta Lake table in Unity Catalog

=== "🐍 Python"

    ```python
    unity_table = unity.load_table("my_catalog_name.my_schema_name.my_table_name")

    df = daft.read_deltalake(unity_table)
    df.show()
    ```

Any subsequent filter operations on the Daft `df` DataFrame object will be correctly optimized to take advantage of DeltaLake features:

=== "🐍 Python"

    ```python
    # Filter which takes advantage of partition pruning capabilities of Delta Lake
    df = df.where(df["partition_key"] < 1000)
    df.show()
    ```

See also [Delta Lake](../io/delta_lake.md) for more information about how to work with the Delta Lake tables provided by the Unity Catalog.

## Downloading files in Unity Catalog volumes

Daft supports downloading from Unity Catalog volumes using [`Expression.url.download()`][daft.expressions.expressions.ExpressionUrlNamespace.download]. File paths that start with `vol+dbfs:/` or `dbfs:/` will be downloaded using the configurations in [`IOConfig.unity`][daft.daft.IOConfig.unity]. These configurations can be created using `UnityCatalog.to_io_config`, or automatically derived from the global session.

=== "🐍 Python"

    ```python
    df = daft.from_pydict({
        "files": [
            "/Volumes/my_catalog/my_schema_name/my_volume_name/file1.txt",
            "/Volumes/my_catalog/my_schema_name/my_volume_name/file2.txt"
        ]
    })

    # explicitly specify the unity catalog
    io_config = unity.to_io_config()
    data_df = df.select("vol+dbfs:" + df["files"].url.download(io_config=io_config))
    data_df.show()

    # use the global session
    from daft.catalog import Catalog
    import daft.session

    catalog = Catalog.from_unity(unity)
    daft.session.attach(catalog)

    data_df = df.select(df["files"].url.download())
    data_df.show()
    ```

## Roadmap

1. Unity Iceberg integration for reading tables using the Iceberg interface instead of the Delta Lake interface

Please make issues on the [Daft repository](https://github.com/Eventual-Inc/Daft) if you have any use-cases that Daft does not currently cover! For the overall Daft development plan, see [Daft Roadmap](../roadmap.md).
