from __future__ import annotations

import dataclasses
from typing import Callable

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

    def _paginate_to_completion(
        self,
        client_func_call: Callable[[unitycatalog.Unitycatalog, str | None], tuple[list[str] | None, str | None]],
    ) -> list[str]:
        results = []

        # Make first request
        new_results, next_page_token = client_func_call(self._client, None)
        if new_results is not None:
            results.extend(new_results)

        # Exhaust pages
        while next_page_token is not None and next_page_token != "":
            new_results, next_page_token = client_func_call(self._client, next_page_token)
            if new_results is not None:
                results.extend(new_results)

        return results

    def list_catalogs(self) -> list[str]:
        def _paginated_list_catalogs(client: unitycatalog.Unitycatalog, page_token: str | None):
            response = client.catalogs.list(page_token=page_token)
            next_page_token = response.next_page_token
            if response.catalogs is None:
                return None, next_page_token
            return [c.name for c in response.catalogs], next_page_token

        return self._paginate_to_completion(_paginated_list_catalogs)

    def list_schemas(self, catalog_name: str) -> list[str]:
        def _paginated_list_schemas(client: unitycatalog.Unitycatalog, page_token: str | None):
            response = client.schemas.list(catalog_name=catalog_name, page_token=page_token)
            next_page_token = response.next_page_token
            if response.schemas is None:
                return None, next_page_token
            return [s.full_name for s in response.schemas], next_page_token

        return self._paginate_to_completion(_paginated_list_schemas)

    def list_tables(self, schema_name: str):
        if schema_name.count(".") != 1:
            raise ValueError(
                f"Expected fully-qualified schema name with format `catalog_name`.`schema_name`, but received: {schema_name}"
            )

        catalog_name, schema_name = schema_name.split(".")

        def _paginated_list_tables(client: unitycatalog.Unitycatalog, page_token: str | None):
            response = client.tables.list(catalog_name=catalog_name, schema_name=schema_name, page_token=page_token)
            next_page_token = response.next_page_token
            if response.tables is None:
                return None, next_page_token
            return [f"{t.catalog_name}.{t.schema_name}.{t.name}" for t in response.tables], next_page_token

        return self._paginate_to_completion(_paginated_list_tables)

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
