# Daft Gravitino Integration

This module provides integration between Daft and Apache Gravitino, an open-source data catalog.

## Overview

Apache Gravitino is a unified metadata management system that provides a single interface to manage metadata across different data sources and storage systems. This integration allows Daft to seamlessly read from and interact with tables and filesets managed by Gravitino.

## Features

- **Catalog Navigation**: List catalogs, schemas, and tables
- **Table Management**: Load existing tables or create new external tables
- **Iceberg Format Support**: Primary support for Apache Iceberg table format
- **Fileset Support**: Access Gravitino filesets for file storage
- **Multi-Cloud Support**: Works with S3, Azure, and GCS storage backends
- **Authentication**: Supports simple and OAuth2 authentication methods
- **Daft Catalog Integration**: Full integration with Daft's catalog system via `Catalog.from_gravitino()` and `Table.from_gravitino()`

## Usage

### Daft Catalog Integration (Recommended)

The easiest way to use Gravitino with Daft is through the integrated catalog system:

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
table = catalog.get_table("my_catalog.my_schema.my_table")
df = table.read()

# Or read directly
df = catalog.read_table("my_catalog.my_schema.my_table")
df.show()
```

### Direct Client Usage

For more advanced use cases, you can use the GravitinoClient directly:

```python
import daft
from daft.gravitino import GravitinoClient

# Initialize client with simple authentication
client = GravitinoClient(
    endpoint="http://localhost:8090",
    metalake_name="my_metalake",
    auth_type="simple",
    username="admin"
)

# Or with OAuth2
client = GravitinoClient(
    endpoint="http://localhost:8090",
    metalake_name="my_metalake",
    auth_type="oauth2",
    token="your-oauth-token"
)
```

### Exploring the Catalog

```python

# List available catalogs in the metalake
catalogs = client.list_catalogs()
print(f"Catalogs: {catalogs}")

# Load catalog details
catalog = client.load_catalog("my_catalog")
print(f"Catalog: {catalog.name}, Type: {catalog.type}, Provider: {catalog.provider}")

# List namespaces in a catalog (namespaces correspond to schemas in Gravitino)
namespaces = client.list_namespaces("my_catalog")
print(f"Namespaces: {namespaces}")

# List tables in a schema
tables = client.list_tables("my_catalog.my_schema")
print(f"Tables: {tables}")
```

### Reading Data

#### Using Daft Catalog (Recommended)

```python
from daft.catalog import Catalog, Table
from daft.gravitino import GravitinoClient

# Create catalog
client = GravitinoClient("http://localhost:8090", "my_metalake", username="admin")
catalog = Catalog.from_gravitino(client)

# Read table using catalog
df = catalog.read_table("my_catalog.my_schema.my_table")
df.show()

# Or get table object first
table = catalog.get_table("my_catalog.my_schema.my_table")
df = table.read()

# Create table from Gravitino table object
gravitino_table = client.load_table("my_catalog.my_schema.my_table")
table = Table.from_gravitino(gravitino_table)
df = table.read()
```

#### Using Direct Client

```python
# Load a table from Gravitino
table = client.load_table("my_catalog.my_schema.my_table")

# Read with Daft based on table format
if table.table_info.format.upper().startswith("ICEBERG"):
    df = daft.read_iceberg(table.table_uri, io_config=table.io_config)
elif table.table_info.format.upper() == "PARQUET":
    df = daft.read_parquet(table.table_uri, io_config=table.io_config)

# Display the data
df.show()
```

### Loading Tables

```python
# Load an existing table
try:
    table = client.load_table("my_catalog.my_schema.existing_table")
    print(f"Loaded table: {table.table_info.name}")
except Exception as e:
    print(f"Table not found: {e}")
```

### Working with Filesets

```python
# Load a fileset
fileset = client.load_fileset("my_catalog.my_schema.my_fileset")

# Access files in the fileset (example with Parquet files)
df = daft.read_parquet(f"{fileset.fileset_info.storage_location}/*.parquet",
                       io_config=fileset.io_config)
```

## Configuration

### Authentication

The client supports two authentication methods:

1. **Simple Authentication**: Uses username/password or just username
2. **OAuth2**: Uses bearer token authentication

### Storage Credentials

Gravitino manages storage credentials through table and fileset properties. The client automatically extracts and configures:

- **S3**: Access key, secret key, and session token
- **Azure**: SAS tokens for blob storage access
- **GCS**: Service account credentials (planned)

## API Reference

### Daft Catalog Integration

#### Catalog.from_gravitino(client)

Creates a Daft Catalog from a GravitinoClient.

```python
from daft.catalog import Catalog
from daft.gravitino import GravitinoClient

client = GravitinoClient("http://localhost:8090", "my_metalake", username="admin")
catalog = Catalog.from_gravitino(client)
```

#### Table.from_gravitino(table)

Creates a Daft Table from a GravitinoTable.

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
- PyIceberg library for reading Iceberg tables: `pip install 'daft[iceberg]'` or `pip install pyiceberg`
- Appropriate cloud storage credentials configured in Gravitino

## Compatibility

This integration supports both legacy and current Gravitino API formats:

- **Legacy format** (pre-1.0): Storage location in `properties.location`
- **Current format** (1.0+): Multiple storage locations in `storageLocations` with configurable default

The client automatically detects and handles both formats for seamless compatibility.

## Limitations

- Credential vending is not yet implemented
- This version directly call Gravitino RESTful API, not using Gravitino Python client
- Some advanced Gravitino features may not be exposed through this client

## Testing

Integration tests are available in `tests/integration/gravitino/`. To run them locally:

```bash
# Start the local Gravitino service
cd tests/integration/gravitino/docker-compose
docker compose up -d

# In a separate shell, run the tests
DAFT_RUNNER=native pytest tests/integration/gravitino -v -m integration
```

You can optionally set `GRAVITINO_TEST_FILE` and `GRAVITINO_TEST_DIR` to point to concrete filesets for the
gvfs:// IO tests.
