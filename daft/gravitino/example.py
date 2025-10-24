"""Example usage of Gravitino client with Daft.

This example demonstrates how to use the GravitinoClient to interact with
Apache Gravitino catalog and read data using Daft.
"""

from __future__ import annotations

import daft
from daft.gravitino import GravitinoClient


def example_gravitino_usage() -> None:
    """Example of using Gravitino with Daft."""
    # Initialize Gravitino client
    client = GravitinoClient(
        endpoint="http://localhost:8090", metalake_name="my_metalake", auth_type="simple", username="admin"
    )

    # List available catalogs
    catalogs = client.list_catalogs()
    print(f"Available catalogs: {catalogs}")

    if catalogs:
        # Load catalog details
        catalog = client.load_catalog(catalogs[0])
        print(f"Catalog details: {catalog.name}, type: {catalog.type}, provider: {catalog.provider}")

        # List schemas in the first catalog
        schemas = client.list_schemas(catalogs[0])
        print(f"Schemas in {catalogs[0]}: {schemas}")

        if schemas:
            # List tables in the first schema
            tables = client.list_tables(schemas[0])
            print(f"Tables in {schemas[0]}: {tables}")

            if tables:
                # Load a table
                table = client.load_table(tables[0])
                print(f"Loaded table: {table.table_info.name}")
                print(f"Storage location: {table.table_uri}")

                # Read the table with Daft (assuming it's an Iceberg table)
                if table.table_info.format.upper().startswith("ICEBERG"):
                    df = daft.read_iceberg(table.table_uri, io_config=table.io_config)
                    print(f"DataFrame schema: {df.schema}")
                elif table.table_info.format.upper() == "PARQUET":
                    df = daft.read_parquet(table.table_uri, io_config=table.io_config)
                    print(f"DataFrame schema: {df.schema}")


def example_load_existing_table() -> None:
    """Example of loading an existing table in Gravitino."""
    client = GravitinoClient(
        endpoint="http://localhost:8090", metalake_name="my_metalake", auth_type="simple", username="admin"
    )

    # Load an existing table
    try:
        table = client.load_table("my_catalog.my_schema.existing_table")
        print(f"Loaded table: {table.table_info.name}")
        print(f"Table type: {table.table_info.table_type}")
        print(f"Storage location: {table.table_uri}")
    except Exception as e:
        print(f"Failed to load table: {e}")


def example_catalog_usage() -> None:
    """Example of working with Gravitino catalogs."""
    client = GravitinoClient(
        endpoint="http://localhost:8090", metalake_name="my_metalake", auth_type="simple", username="admin"
    )

    # Load a catalog
    catalog = client.load_catalog("my_catalog")
    print(f"Loaded catalog: {catalog.name}")
    print(f"Catalog type: {catalog.type}")
    print(f"Catalog provider: {catalog.provider}")
    print(f"Catalog properties: {catalog.properties}")


def example_fileset_usage() -> None:
    """Example of working with Gravitino filesets."""
    client = GravitinoClient(
        endpoint="http://localhost:8090", metalake_name="my_metalake", auth_type="simple", username="admin"
    )

    # Load a fileset
    fileset = client.load_fileset("my_catalog.my_schema.my_fileset")
    print(f"Loaded fileset: {fileset.fileset_info.name}")
    print(f"Storage location: {fileset.fileset_info.storage_location}")

    # Access files in the fileset (example with Parquet files)
    df = daft.read_parquet(f"{fileset.fileset_info.storage_location}/*.parquet", io_config=fileset.io_config)
    print(f"DataFrame schema: {df.schema}")


if __name__ == "__main__":
    # Run examples (commented out since they require a running Gravitino server)
    # example_gravitino_usage()
    # example_load_existing_table()
    # example_catalog_usage()
    # example_fileset_usage()

    print("Gravitino client examples ready to run!")
    print("Make sure you have a Gravitino server running at http://localhost:8090")
