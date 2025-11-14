# Reading from Apache Gravitino

[Apache Gravitino](https://gravitino.apache.org/) is an open-source data catalog that provides unified metadata management for various data sources and storage systems. Users of Gravitino can work with data assets such as tables (Iceberg, Hive, etc.) and filesets (storing raw files, on s3, gcs, azure blob, etc).

To use Daft with Gravitino, you will need to install Daft with the `gravitino` option specified like so:

```bash
pip install daft[gravitino]
```

!!! warning "Warning"

    These APIs are in beta and may be subject to change as the Gravitino connector continues to be developed.

## Connecting to Gravitino

Daft includes an abstraction for the Gravitino catalog.

=== "🐍 Python"

    ```python
    from daft.gravitino import GravitinoClient

    gravitino = GravitinoClient(
        endpoint="http://localhost:8090",
        metalake_name="my_metalake",
        auth_type="simple",
        username="admin",
    )

    # See all available catalogs
    print(gravitino.list_catalogs())

    # See available schemas in a given catalog
    print(gravitino.list_schemas("my_catalog"))

    # See available tables in a given schema
    print(gravitino.list_tables("my_catalog.my_schema"))
    ```


## Reading files from Gravitino filesets

Gravitino filesets allow you to manage collections of files. Daft supports reading from filesets using the `gvfs://` URL scheme. Before this, make sure you already define the fileset in Gravitino with a storage location like s3. Refer to Gravitino's [documentation](https://gravitino.apache.org/docs/1.0.0/fileset-catalog-with-s3) for details.

=== "🐍 Python"

    ```python
    from daft.io import IOConfig, GravitinoConfig

    # Create IOConfig with Gravitino configuration
    io_config = IOConfig(
        gravitino=GravitinoConfig(
            endpoint="http://localhost:8090",
            metalake_name="my_metalake",
            auth_type="simple",
        )
    )

    # Read files from a fileset using gvfs:// URL
    # Format: gvfs://fileset/catalog/schema/fileset/path
    df = daft.read_parquet(
        "gvfs://fileset/my_catalog/my_schema/my_fileset/data.parquet",
        io_config=io_config
    )
    df.show()
    ```

You can also use glob patterns to read multiple files:

=== "🐍 Python"

    ```python
    # Read all parquet files in a fileset
    df = daft.read_parquet(
        "gvfs://fileset/my_catalog/my_schema/my_fileset/**/*.parquet",
        io_config=io_config
    )
    df.show()
    ```


## Roadmap

1. Support for read/write Iceberg tables from Gravitino
2. Support for additional table formats (Hive, Hudi)
3. Support for more storages (gcs, azure adls, oss, etc)
3. Support for credential vending

Please open issues on the [Daft repository](https://github.com/Eventual-Inc/Daft) or [Gravitino repository](https://github.com/apache/gravitino) if you have any use-cases that Daft Gravitino connector does not currently cover!
