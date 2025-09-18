from __future__ import annotations

import dataclasses
import warnings
from typing import TYPE_CHECKING, Callable, Literal
from urllib.parse import urlparse

import unitycatalog

from daft.io import AzureConfig, IOConfig, S3Config, UnityConfig

if TYPE_CHECKING:
    from unitycatalog.types import (
        GenerateTemporaryTableCredentialResponse,
        GenerateTemporaryVolumeCredentialResponse,
        TableInfo,
        VolumeInfo,
    )


@dataclasses.dataclass(frozen=True)
class UnityCatalogTable:
    table_info: TableInfo
    table_uri: str
    io_config: IOConfig | None


@dataclasses.dataclass(frozen=True)
class UnityCatalogVolume:
    volume_info: VolumeInfo
    io_config: IOConfig | None


def _io_config_from_temp_creds(
    temp_creds: GenerateTemporaryTableCredentialResponse | GenerateTemporaryVolumeCredentialResponse,
    storage_location: str,
) -> IOConfig | None:
    scheme = urlparse(storage_location).scheme
    if scheme == "s3" or scheme == "s3a":
        aws_temp_credentials = temp_creds.aws_temp_credentials
        return (
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
    elif scheme == "gcs" or scheme == "gs":
        # TO-DO: gather GCS credential vending assets from Unity and construct 'io_config``
        warnings.warn("GCS credential vending from Unity Catalog is not yet supported.")
        return None
    elif scheme == "az" or scheme == "abfs" or scheme == "abfss":
        return IOConfig(azure=AzureConfig(sas_token=temp_creds.azure_user_delegation_sas.get("sas_token")))
    else:
        warnings.warn(f"Credentials for scheme {scheme} are not yet supported.")
        return None


class UnityCatalog:
    """Client to access the Unity Catalog.

    Unity Catalog is an open-sourced data catalog that can be self-hosted, or hosted by Databricks.

    Example of reading a dataframe from a table in Unity Catalog hosted by Databricks:

    >>> cat = UnityCatalog("https://<databricks_workspace_id>.cloud.databricks.com", token="my-token")
    >>> table = cat.load_table("my_catalog.my_schema.my_table")
    >>> df = daft.read_deltalake(table)
    """

    def __init__(self, endpoint: str, token: str | None = None):
        self._endpoint = endpoint
        self._token = token
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
        def _paginated_list_catalogs(
            client: unitycatalog.Unitycatalog, page_token: str | None
        ) -> tuple[list[str] | None, str | None]:
            response = client.catalogs.list(page_token=page_token)
            next_page_token = response.next_page_token
            if response.catalogs is None:
                return None, next_page_token
            return [c.name for c in response.catalogs], next_page_token

        return self._paginate_to_completion(_paginated_list_catalogs)

    def list_schemas(self, catalog_name: str) -> list[str]:
        def _paginated_list_schemas(
            client: unitycatalog.Unitycatalog, page_token: str | None
        ) -> tuple[list[str] | None, str | None]:
            response = client.schemas.list(catalog_name=catalog_name, page_token=page_token)
            next_page_token = response.next_page_token
            if response.schemas is None:
                return None, next_page_token
            return [s.full_name for s in response.schemas], next_page_token

        return self._paginate_to_completion(_paginated_list_schemas)

    def list_tables(self, schema_name: str) -> list[str]:
        if schema_name.count(".") != 1:
            raise ValueError(
                f"Expected fully-qualified schema name with format `catalog_name`.`schema_name`, but received: {schema_name}"
            )

        catalog_name, schema_name = schema_name.split(".")

        def _paginated_list_tables(
            client: unitycatalog.Unitycatalog, page_token: str | None
        ) -> tuple[list[str] | None, str | None]:
            response = client.tables.list(catalog_name=catalog_name, schema_name=schema_name, page_token=page_token)
            next_page_token = response.next_page_token
            if response.tables is None:
                return None, next_page_token
            return [f"{t.catalog_name}.{t.schema_name}.{t.name}" for t in response.tables], next_page_token

        return self._paginate_to_completion(_paginated_list_tables)

    def load_table(
        self,
        table_name: str,
        new_table_storage_path: str | None = None,
        operation: Literal["READ", "READ_WRITE"] = "READ_WRITE",
        table_type: Literal["EXTERNAL", "MANAGED"] = "EXTERNAL",
    ) -> UnityCatalogTable:
        """Loads an existing Unity Catalog table. If the table is not found, and information is provided in the method to create a new table, a new table will be attempted to be registered.

        Args:
            table_name (str): Name of the table in Unity Catalog in the form of dot-separated, 3-level namespace
            new_table_storage_path (str, optional): Cloud storage path URI to register a new external table using this path. Unity Catalog will validate if the path is valid and authorized for the principal, else will raise an exception.
            operation ("READ" or "READ_WRITE", optional): The intended use of the table, which impacts authorization. Defaults to "READ_WRITE".
                table_type ("EXTERNAL" or "MANAGED", optional): The table type impacts the creation of the table if it does not exist. If "MANAGED" the table is created as managed table. If "EXTERNAL" the table is created based on the new_tabke_storage_path.

        Returns:
            UnityCatalogTable
        """
        # Load the table ID
        try:
            table_info = self._client.tables.retrieve(table_name)
            if new_table_storage_path:
                warnings.warn(
                    f"Table {table_name} is an existing storage table with a valid storage path. The 'new_table_storage_path' argument provided will be ignored."
                )
        except unitycatalog.NotFoundError:
            if table_type == "EXTERNAL" and not new_table_storage_path:
                raise ValueError(
                    f"Table {table_name} is not an existing table. If a new table needs to be created, provide 'new_table_storage_path' value or set table_type to 'MANAGED'."
                )
            elif table_type == "MANAGED":
                warnings.warn(f"Table {table_name} is being created as managed table in Unity Catalog.")
            try:
                three_part_namesplit = table_name.split(".")
                if len(three_part_namesplit) != 3 or not all(three_part_namesplit):
                    raise ValueError(
                        f"Expected table name to be in the format of 'catalog.schema.table', received: {table_name}"
                    )

                params = {
                    "catalog_name": three_part_namesplit[0],
                    "schema_name": three_part_namesplit[1],
                    "name": three_part_namesplit[2],
                    "columns": None,
                    "data_source_format": "DELTA",
                    "table_type": table_type,
                    "storage_location": new_table_storage_path,
                    "comment": None,
                }

                table_info = self._client.tables.create(**params)
            except Exception as e:
                raise Exception(f"An error occurred while registering the table in Unity Catalog: {e}")

        table_id = table_info.table_id
        storage_location = table_info.storage_location
        # Grab credentials from Unity catalog and place it into the Table
        temp_table_credentials = self._client.temporary_table_credentials.create(operation=operation, table_id=table_id)

        io_config = _io_config_from_temp_creds(temp_table_credentials, storage_location)

        return UnityCatalogTable(
            table_info=table_info,
            table_uri=storage_location,
            io_config=io_config,
        )

    def load_volume(
        self, name: str, operation: Literal["READ_VOLUME", "WRITE_VOLUME"] = "READ_VOLUME"
    ) -> UnityCatalogVolume:
        volume_info = self._client.volumes.retrieve(name)
        temp_volume_credentials = self._client.temporary_volume_credentials.create(
            operation=operation, volume_id=volume_info.volume_id
        )

        io_config = _io_config_from_temp_creds(temp_volume_credentials, volume_info.storage_location)

        return UnityCatalogVolume(volume_info=volume_info, io_config=io_config)

    def to_io_config(self) -> IOConfig:
        return IOConfig(unity=UnityConfig(endpoint=self._endpoint, token=self._token))
