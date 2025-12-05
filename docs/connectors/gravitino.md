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
- **Table Management**: Load existing tables or create new external tables
- **Fileset Support**: Access Gravitino filesets for file storage
- **Authentication**: Supports simple and OAuth2 authentication methods
- **Daft Catalog Integration**: Integration with Daft's catalog system via `Catalog.from_gravitino()` and `Table.from_gravitino()`

## Connecting to Gravitino

### Using Daft Catalog Integration (Recommended)

The easiest way to use Gravitino with Daft is through the integrated catalog system:

=== "🐍 Python"

    ```python
    import daft
    from daft.catalog import Catalog
    from daft.gravitino import GravitinoClient

    # Create Gravitino client
    client = GravitinoClient(
        endpoint="http://localhost:8090",
        metalake_name="my_metalake",
        auth_type="simple",
        username="admin"
    )

    # Create Daft catalog from Gravitino client
    catalog = Catalog.from_gravitino(client)

    # Use standard Daft catalog operations
    tables = catalog.list_tables()
    ```

### Using Direct Client

For more advanced use cases, you can use the GravitinoClient directly:

=== "🐍 Python"

    ```python
    import daft
    from daft.gravitino import GravitinoClient

    # Initialize client with simple authentication
    client = GravitinoClient(
        endpoint="http://localhost:8090",
        metalake_name="my_metalake",
        auth_type="simple",
        username="admin",
    )

    # See all available catalogs
    print(client.list_catalogs())

    # See available schemas in a given catalog
    print(client.list_namespaces("my_catalog"))

    # See available tables in a given schema
    print(client.list_tables("my_catalog.my_schema"))
    ```

## Configuration

### Authentication

The client supports two authentication methods:

1. **Simple Authentication**: Uses username/password or just username
2. **OAuth2**: Uses bearer token authentication

### Storage Credentials

Gravitino manages storage credentials through table and fileset properties. The client automatically extracts and configures:

- **S3**: Access key, secret key, and session token

## API Reference

### Daft Catalog Integration

#### Catalog.from_gravitino(client)

Creates a Daft Catalog from a GravitinoClient.

=== "🐍 Python"

    ```python
    from daft.catalog import Catalog
    from daft.gravitino import GravitinoClient

    client = GravitinoClient("http://localhost:8090", "my_metalake", username="admin")
    catalog = Catalog.from_gravitino(client)
    ```

#### Table.from_gravitino(table)

Creates a Daft Table from a GravitinoTable.

=== "🐍 Python"

    ```python
    from daft.catalog import Table

    gravitino_table = client.load_table("my_catalog.my_schema.my_table")
    table = Table.from_gravitino(gravitino_table)
    ```

### GravitinoClient

Main client class for interacting with Gravitino.

**Methods:**

- `list_catalogs()` - List all catalogs in the metalake
- `load_catalog(catalog_name)` - Load catalog details
- `list_namespaces(catalog_name)` - List namespaces in a catalog (namespaces correspond to schemas in Gravitino)
- `list_tables(namespace_name)` - List tables in a namespace
- `load_table(table_name)` - Load an existing table
- `load_fileset(fileset_name)` - Load a fileset
- `to_io_config()` - Get IOConfig for the client

### GravitinoTable

Represents a table in Gravitino with metadata and storage configuration.

**Attributes:**

- `table_info` - Table metadata (name, type, format, etc.)
- `table_uri` - Storage location URI
- `io_config` - Daft IOConfig for accessing the table

### GravitinoCatalog

Represents a catalog in Gravitino.

**Attributes:**

- `name` - Catalog name
- `type` - Catalog type (e.g., "relational", "fileset")
- `provider` - Catalog provider (e.g., "hive", "iceberg", "hadoop")
- `properties` - Catalog configuration properties

### GravitinoFileset

Represents a fileset in Gravitino for file storage.

**Attributes:**

- `fileset_info` - Fileset metadata
- `io_config` - Daft IOConfig for accessing the fileset

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
- Some advanced Gravitino features may not be exposed through this client

## Roadmap

1. Support for read/write Iceberg tables from Gravitino
2. Support for additional table formats (Hive, Hudi)
3. Support for more storages (gcs, azure adls, oss, etc)
4. Support for credential vending

Please open issues on the [Daft repository](https://github.com/Eventual-Inc/Daft) or [Gravitino repository](https://github.com/apache/gravitino) if you have any use-cases that Daft Gravitino connector does not currently cover!
