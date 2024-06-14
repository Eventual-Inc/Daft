from __future__ import annotations

import dataclasses

import unitycatalog

from daft.io import IOConfig, S3Config


@dataclasses.dataclass(frozen=True)
class UnityCatalogTable:
    table_uri: str
    io_config: IOConfig | None


class UnityCatalog:
    """Client to access the Unity Catalog

    Unity Catalog is an open-sourced data catalog that can be self-hosted, or hosted by Databricks.

    Example of reading a dataframe from a table in Unity Catalog hosted by Databricks:

    >>> cat = UnityCatalog("https://<databricks_workspace_id>.cloud.databricks.com", token="my-token")
    >>> table = cat.load_table("my_catalog.my_schema.my_table")
    >>> df = daft.read_delta_lake(table)
    """

    def __init__(self, endpoint: str, token: str | None = None):
        self._client = unitycatalog.Unitycatalog(
            base_url=endpoint.rstrip("/") + "/api/2.1/unity-catalog/",
            default_headers={"Authorization": f"Bearer {token}"},
        )

    def list_catalogs(self) -> list[str]:
        catalog_names = []

        # Make first request
        response = self._client.catalogs.list()
        if response.catalogs is not None:
            catalog_names.extend([c.name for c in response.catalogs])

        # Exhaust pages
        while response.next_page_token is not None and response.next_page_token != "":
            response = self._client.catalogs.list(page_token=response.next_page_token)
            if response.catalogs is not None:
                catalog_names.extend([c.name for c in response.catalogs])

        return catalog_names

    def list_schemas(self, catalog_name: str) -> list[str]:
        schema_names = []

        # Make first request
        response = self._client.schemas.list(catalog_name=catalog_name)
        if response.schemas is not None:
            schema_names.extend([s.full_name for s in response.schemas])

        # Exhaust pages
        while response.next_page_token is not None and response.next_page_token != "":
            response = self._client.schemas.list(catalog_name=catalog_name, page_token=response.next_page_token)
            if response.schemas is not None:
                schema_names.extend([s.full_name for s in response.schemas])

        return schema_names

    def list_tables(self, schema_name: str):
        if schema_name.count(".") != 1:
            raise ValueError(
                f"Expected fully-qualified schema name with format `catalog_name`.`schema_name`, but received: {schema_name}"
            )

        catalog_name, schema_name = schema_name.split(".")
        table_names = []

        # Make first request
        response = self._client.tables.list(catalog_name=catalog_name, schema_name=schema_name)
        if response.tables is not None:
            table_names.extend([f"{t.catalog_name}.{t.schema_name}.{t.name}" for t in response.tables])

        # Exhaust pages
        while response.next_page_token is not None and response.next_page_token != "":
            response = self._client.tables.list(
                catalog_name=catalog_name, schema_name=schema_name, page_token=response.next_page_token
            )
            if response.tables is not None:
                table_names.extend([f"{t.catalog_name}.{t.schema_name}.{t.name}" for t in response.tables])

        return table_names

    def load_table(self, table_name: str) -> UnityCatalogTable:
        # Load the table ID
        table_info = self._client.tables.retrieve(table_name)
        table_id = table_info.table_id
        storage_location = table_info.storage_location

        # Grab credentials from Unity catalog and place it into the Table
        temp_table_credentials = self._client.temporary_table_credentials.create(operation="READ", table_id=table_id)
        aws_temp_credentials = temp_table_credentials.aws_temp_credentials
        io_config = (
            IOConfig(
                s3=S3Config(
                    key_id=aws_temp_credentials.access_key_id,
                    access_key=aws_temp_credentials.secret_access_key,
                    session_token=aws_temp_credentials.session_token,
                )
            )
            if aws_temp_credentials is not None
            else None
        )

        return UnityCatalogTable(
            table_uri=storage_location,
            io_config=io_config,
        )
