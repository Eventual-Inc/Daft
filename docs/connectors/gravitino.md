# Reading from Apache Gravitino

[Apache Gravitino](https://gravitino.apache.org/) is an open-source data catalog that provides unified metadata management for various data sources and storage systems. Users of Gravitino can work with data assets such as tables (Iceberg, Hive, etc.) and filesets (storing raw files, on s3, gcs, azure blob, etc).

To use Daft with Gravitino, you will need to install Daft with the `gravitino` option specified like so:

```bash
pip install daft[gravitino]
```

!!! warning "Warning"

    These APIs are in beta and may be subject to change as the Gravitino connector continues to be developed.

## Features

- **Catalog Navigation**: List catalogs, schemas, and tables
- **Multi-Format Tables**: Read Iceberg and Hive/Parquet tables via `catalog.get_table("...").read()`
- **Table Management**: Load existing tables or create new external tables
- **Fileset Support**: Access Gravitino filesets for file storage
- **GVFS Protocol**: Read and write files using `gvfs://` URLs for seamless fileset access
- **Authentication**: Supports simple and OAuth2 authentication methods
- **Daft Catalog Integration**: Integration with Daft's catalog system via `Catalog.from_gravitino()`

## Connecting to Gravitino

=== "🐍 Python"

    ```python
    import daft
    from daft.catalog import Catalog

    catalog = Catalog.from_gravitino(
        endpoint="http://localhost:8090",
        metalake_name="my_metalake",
        username="admin",
    )

    # List all available tables
    tables = catalog.list_tables("my_catalog.my_schema")

    # Read a table (format detected automatically)
    df = catalog.get_table("my_catalog.my_schema.my_table").read()
    df.show()
    ```

## Configuration

### Authentication

`Catalog.from_gravitino` supports two authentication methods:

1. **Simple Authentication**: Uses username/password or just username
2. **OAuth2**: Uses bearer token authentication

=== "🐍 Python"

    ```python
    from daft.catalog import Catalog

    # Simple auth with username only
    catalog = Catalog.from_gravitino(
        endpoint="http://localhost:8090",
        metalake_name="my_metalake",
        auth_type="simple",
        username="admin",
    )

    # OAuth2 auth
    catalog = Catalog.from_gravitino(
        endpoint="http://localhost:8090",
        metalake_name="my_metalake",
        auth_type="oauth2",
        token="my-bearer-token",
    )
    ```

### Storage Credentials

Gravitino manages storage credentials through table and fileset properties. The client automatically extracts and configures:

- **S3**: Access key, secret key, and session token

## GVFS Protocol Support

Daft supports reading and writing files directly from Gravitino filesets using the `gvfs://` protocol. This provides a unified interface for accessing files stored in various cloud storage systems through Gravitino's metadata management.

### GVFS URL Format

GVFS URLs follow this format:
```
gvfs://fileset/<catalog>/<schema>/<fileset>/<path>
```

Where:
- `<catalog>` - Name of the Gravitino catalog
- `<schema>` - Name of the schema within the catalog
- `<fileset>` - Name of the fileset
- `<path>` - Optional path to specific files within the fileset

### Reading Files with GVFS

=== "🐍 Python"

    ```python
    import daft
    from daft.io import IOConfig, GravitinoConfig

    # Build an IOConfig for GVFS fileset access
    io_config = IOConfig(
        gravitino=GravitinoConfig(
            endpoint="http://localhost:8090",
            metalake_name="my_metalake",
            username="admin",
        )
    )

    # Read parquet files from a fileset
    df = daft.read_parquet(
        "gvfs://fileset/my_catalog/my_schema/my_fileset/**/*.parquet",
        io_config=io_config
    )

    # Read specific file
    df = daft.read_parquet(
        "gvfs://fileset/my_catalog/my_schema/my_fileset/data.parquet",
        io_config=io_config
    )

    # Read CSV files
    df = daft.read_csv(
        "gvfs://fileset/my_catalog/my_schema/my_fileset/*.csv",
        io_config=io_config
    )

    # Use glob patterns for file discovery
    files_df = daft.from_glob_path(
        "gvfs://fileset/my_catalog/my_schema/my_fileset/**/*.json",
        io_config=io_config
    )
    ```

### Writing Files with GVFS

=== "🐍 Python"

    ```python
    import daft
    from daft.io import IOConfig, GravitinoConfig

    io_config = IOConfig(
        gravitino=GravitinoConfig(
            endpoint="http://localhost:8090",
            metalake_name="my_metalake",
            username="admin",
        )
    )

    # Create sample data
    df = daft.from_pydict({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35]
    })

    # Write parquet files to a fileset
    df.write_parquet(
        "gvfs://fileset/my_catalog/my_schema/my_fileset/output.parquet",
        io_config=io_config
    )

    # Write CSV files
    df.write_csv(
        "gvfs://fileset/my_catalog/my_schema/my_fileset/output.csv",
        io_config=io_config
    )

    # Write JSON files
    df.write_json(
        "gvfs://fileset/my_catalog/my_schema/my_fileset/output.json",
        io_config=io_config
    )
    ```

### GVFS Benefits

- **Unified Access**: Use the same URL format for reading and writing
- **Storage Abstraction**: Access files without knowing underlying storage details (S3, GCS, etc.)
- **Metadata Integration**: Leverage Gravitino's catalog metadata for data discovery
- **Credential Management**: Gravitino handles storage credentials automatically
- **Multi-format Support**: Works with Parquet, CSV, JSON, and other file formats

## API Reference

### Catalog.from_gravitino(...)

Creates a Daft Catalog from a Gravitino metalake.

=== "🐍 Python"

    ```python
    from daft.catalog import Catalog

    catalog = Catalog.from_gravitino(
        endpoint="http://localhost:8090",
        metalake_name="my_metalake",
        username="admin",
    )
    catalog.list_tables("my_catalog.my_schema")
    ```

#### Table.from_gravitino(table)

Creates a Daft Table from a GravitinoTable.

=== "🐍 Python"

    ```python
    from daft.catalog import Catalog, Table

    catalog = Catalog.from_gravitino(
        endpoint="http://localhost:8090",
        metalake_name="my_metalake",
        username="admin",
    )
    table = catalog.get_table("my_catalog.my_schema.my_table")
    df = table.read()
    ```

## Requirements

- Apache Gravitino server (0.9.0+)
- Python requests library
- Appropriate cloud storage credentials configured in Gravitino

## Compatibility

This integration supports both legacy and current Gravitino API formats:

- **Legacy format** (pre-1.0): Storage location in `properties.location`
- **Current format** (1.0+): Multiple storage locations in `storageLocations` with configurable default

The client automatically detects and handles both formats for seamless compatibility.

## Limitations

- Credential vending is not yet implemented
- This version directly calls Gravitino RESTful API, not using Gravitino Python client
- GVFS write support currently works with S3-backed filesets (other storage backends coming soon)
- Some advanced Gravitino features may not be exposed through this client

## Roadmap

1. Support for read/write Iceberg tables from Gravitino ✓
2. Support for Hive/Parquet tables ✓
3. Support for additional table formats (Hudi)
4. Support for more storages (gcs, azure adls, oss, etc)
5. Support for credential vending

Please open issues on the [Daft repository](https://github.com/Eventual-Inc/Daft) or [Gravitino repository](https://github.com/apache/gravitino) if you have any use-cases that Daft Gravitino connector does not currently cover!
