# isort: dont-add-import: from __future__ import annotations

import os
from typing import Optional, Union

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, NativeStorageConfig, ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.io.catalog import DataCatalogTable, DataCatalogType
from daft.logical.builder import LogicalPlanBuilder


@PublicAPI
def read_delta_lake(
    table: Union[str, DataCatalogTable],
    io_config: Optional["IOConfig"] = None,
    _multithreaded_io: Optional[bool] = None,
) -> DataFrame:
    """Create a DataFrame from a Delta Lake table.

    Example:
        >>> df = daft.read_delta_lake("some-table-uri")
        >>>
        >>> # Filters on this dataframe can now be pushed into
        >>> # the read operation from Delta Lake.
        >>> df = df.where(df["foo"] > 5)
        >>> df.show()

    .. NOTE::
        This function requires the use of `deltalake <https://delta-io.github.io/delta-rs/>`_, a Python library for
        interacting with Delta Lake.

    Args:
        table: Either a URI for the Delta Lake table or a :class:`~daft.io.catalog.DataCatalogTable` instance
            referencing a table in a data catalog, such as AWS Glue Data Catalog or Databricks Unity Catalog.
        io_config: A custom :class:`~daft.daft.IOConfig` to use when accessing Delta Lake object storage data. Defaults to None.
        _multithreaded_io: Whether to use multithreading for IO threads. Setting this to False can be helpful in reducing
            the amount of system resources (number of connections and thread contention) when running in the Ray runner.
            Defaults to None, which will let Daft decide based on the runner it is currently using.

    Returns:
        DataFrame: A DataFrame with the schema converted from the specified Delta Lake table.
    """
    from daft.delta_lake.delta_lake_scan import DeltaLakeScanOperator

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    # If running on Ray, we want to limit the amount of concurrency and requests being made.
    # This is because each Ray worker process receives its own pool of thread workers and connections
    multithreaded_io = not context.get_context().is_ray_runner if _multithreaded_io is None else _multithreaded_io
    storage_config = StorageConfig.native(NativeStorageConfig(multithreaded_io, io_config))

    if isinstance(table, str):
        table_uri = table
    elif isinstance(table, DataCatalogTable):
        assert table.catalog == DataCatalogType.GLUE or table.catalog == DataCatalogType.UNITY
        if table.catalog == DataCatalogType.GLUE:
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
            if table.catalog_id is not None:
                # Allow cross account access, table.catalog_id should be the target account id
                glue_table = glue.get_table(
                    CatalogId=table.catalog_id, DatabaseName=table.database_name, Name=table.table_name
                )
            else:
                glue_table = glue.get_table(DatabaseName=table.database_name, Name=table.table_name)

            # TODO(Clark): Fetch more than just the table URI from Glue Data Catalog.
            table_uri = glue_table["Table"]["StorageDescriptor"]["Location"]
        elif table.catalog == DataCatalogType.UNITY:
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
            catalog_id = table.catalog_id
            # TODO(Clark): Use default (workspace) catalog if no catalog ID is specified?
            if catalog_id is None:
                raise ValueError("DataCatalogTable.catalog_id must be set for reading from Unity catalogs.")
            database_name = table.database_name
            table_name = table.table_name
            full_table_name = f"{catalog_id}.{database_name}.{table_name}"
            unity_table = workspace_client.tables.get(f"{catalog_url}/tables/{full_table_name}")
            # TODO(Clark): Propagate more than just storage location.
            table_uri = unity_table.storage_location
            if table_uri is None:
                raise ValueError(f"Storage location is missing from Unity Catalog table: {unity_table}")
    else:
        raise ValueError(
            f"table argument must be a table URI string or a DataCatalogTable instance, but got: {type(table)}, {table}"
        )
    delta_lake_operator = DeltaLakeScanOperator(table_uri, storage_config=storage_config)

    handle = ScanOperatorHandle.from_python_scan_operator(delta_lake_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)
