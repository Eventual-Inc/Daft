# isort: dont-add-import: from __future__ import annotations

from typing import Optional, Union

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, NativeStorageConfig, ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.io.catalog import DataCatalog, DataCatalogTable
from daft.logical.builder import LogicalPlanBuilder


@PublicAPI
def read_delta_lake(
    table: Union[str, DataCatalogTable],
    io_config: Optional["IOConfig"] = None,
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

    Returns:
        DataFrame: A DataFrame with the schema converted from the specified Delta Lake table.
    """
    from daft.delta_lake.delta_lake_scan import DeltaLakeScanOperator

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    multithreaded_io = not context.get_context().is_ray_runner
    storage_config = StorageConfig.native(NativeStorageConfig(multithreaded_io, io_config))

    if isinstance(table, str):
        table_uri = table
    elif isinstance(table, DataCatalogTable):
        from deltalake import DataCatalog as DeltaDataCatalog
        from deltalake import DeltaTable

        assert table.catalog == DataCatalog.GLUE or table.catalog == DataCatalog.UNITY
        if table.catalog == DataCatalog.GLUE:
            delta_data_catalog = DeltaDataCatalog.AWS
        elif table.catalog == DataCatalog.UNITY:
            delta_data_catalog = DeltaDataCatalog.UNITY
        delta_table = DeltaTable.from_data_catalog(
            data_catalog=delta_data_catalog,
            database_name=table.database_name,
            table_name=table.table_name,
            data_catalog_id=table.catalog_id,
        )
        table_uri = delta_table.table_uri
    else:
        raise ValueError(
            f"table argument must be a table URI string or a DataCatalogTable instance, but got: {type(table)}, {table}"
        )
    delta_lake_operator = DeltaLakeScanOperator(table_uri, storage_config=storage_config)

    handle = ScanOperatorHandle.from_python_scan_operator(delta_lake_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)
