# isort: dont-add-import: from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from daft.daft import IOConfig


class DataCatalogType(Enum):
    """Supported data catalogs."""

    GLUE = "glue"
    """
    AWS Glue Data Catalog.
    """
    UNITY = "unity"
    """
    Databricks Unity Catalog.
    """


@dataclass
class DataCatalogTable:
    """
    A reference to a table in some database in some data catalog.

    See :class:`~.DataCatalog`
    """

    catalog: DataCatalogType
    database_name: str
    table_name: str
    catalog_id: Optional[str] = None

    def table_uri(self, io_config: IOConfig) -> str:
        """
        Get the URI of the table in the data catalog.

        Returns:
            str: The URI of the table.
        """
        if self.catalog == DataCatalogType.GLUE:
            # Use boto3 to get the table from AWS Glue Data Catalog.
            import boto3

            s3_config = io_config.s3

            glue = boto3.client(
                "glue",
                region_name=s3_config.region_name,
                use_ssl=s3_config.use_ssl,
                verify=s3_config.verify_ssl,
                endpoint_url=s3_config.endpoint_url,
                aws_access_key_id=s3_config.key_id,
                aws_secret_access_key=s3_config.access_key,
                aws_session_token=s3_config.session_token,
            )
            if self.catalog_id is not None:
                # Allow cross account access, table.catalog_id should be the target account id
                glue_table = glue.get_table(
                    CatalogId=self.catalog_id, DatabaseName=self.database_name, Name=self.table_name
                )
            else:
                glue_table = glue.get_table(DatabaseName=self.database_name, Name=self.table_name)

            # TODO(Clark): Fetch more than just the table URI from Glue Data Catalog.
            return glue_table["Table"]["StorageDescriptor"]["Location"]
        elif self.catalog == DataCatalogType.UNITY:
            import os

            # Use Databricks SDK to get the table from the Unity Catalog.
            from databricks.sdk import WorkspaceClient

            # TODO(Clark): Populate WorkspaceClient with user-facing catalog configs (with some fields sourced from
            # IOConfig) rather than relying on environment variable configuration.
            workspace_client = WorkspaceClient()
            # TODO(Clark): Expose Databricks/Unity host as user-facing catalog config.
            try:
                workspace_url = os.environ["DATABRICKS_HOST"]
            except KeyError:
                raise ValueError(
                    "DATABRICKS_HOST or UNITY_HOST environment variable must be set for Daft to create a workspace URL."
                )
            catalog_url = f"{workspace_url}/api/2.1/unity-catalog"
            catalog_id = self.catalog_id
            # TODO(Clark): Use default (workspace) catalog if no catalog ID is specified?
            if catalog_id is None:
                raise ValueError("DataCatalogTable.catalog_id must be set for reading from Unity catalogs.")
            database_name = self.database_name
            table_name = self.table_name
            full_table_name = f"{catalog_id}.{database_name}.{table_name}"
            unity_table = workspace_client.tables.get(f"{catalog_url}/tables/{full_table_name}")
            # TODO(Clark): Propagate more than just storage location.
            if unity_table.storage_location is None:
                raise ValueError(f"Storage location is missing from Unity Catalog table: {unity_table}")
            return unity_table.storage_location
        assert False, f"Unsupported catalog: {self.catalog}"
