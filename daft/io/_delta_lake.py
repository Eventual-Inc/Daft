# isort: dont-add-import: from __future__ import annotations

import warnings
from typing import Optional, Union

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, NativeStorageConfig, ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.io.catalog import DataCatalogTable
from daft.logical.builder import LogicalPlanBuilder

_UNITY_CATALOG_AVAILABLE = True
try:
    from daft.unity_catalog import UnityCatalogTable
except ImportError:
    _UNITY_CATALOG_AVAILABLE = False


def read_delta_lake(
    table: Union[str, DataCatalogTable],
    io_config: Optional["IOConfig"] = None,
    _multithreaded_io: Optional[bool] = None,
) -> DataFrame:
    warnings.warn(
        "read_delta_lake has been renamed to read_deltalake and will be removed in Daft v0.3",
        DeprecationWarning,
    )
    return read_deltalake(table, io_config, _multithreaded_io)


@PublicAPI
def read_deltalake(
    table: Union[str, DataCatalogTable, "UnityCatalogTable"],
    io_config: Optional["IOConfig"] = None,
    _multithreaded_io: Optional[bool] = None,
) -> DataFrame:
    """Create a DataFrame from a Delta Lake table.

    Example:
        >>> df = daft.read_deltalake("some-table-uri")
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

    # If running on Ray, we want to limit the amount of concurrency and requests being made.
    # This is because each Ray worker process receives its own pool of thread workers and connections
    multithreaded_io = not context.get_context().is_ray_runner if _multithreaded_io is None else _multithreaded_io

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_config = StorageConfig.native(NativeStorageConfig(multithreaded_io, io_config))

    if isinstance(table, str):
        table_uri = table
    elif isinstance(table, DataCatalogTable):
        table_uri = table.table_uri(io_config)
    elif _UNITY_CATALOG_AVAILABLE and isinstance(table, UnityCatalogTable):
        table_uri = table.table_uri

        # Override the storage_config with the one provided by Unity catalog
        table_io_config = table.io_config
        if table_io_config is not None:
            storage_config = StorageConfig.native(NativeStorageConfig(multithreaded_io, table_io_config))
    else:
        raise ValueError(
            f"table argument must be a table URI string, DataCatalogTable or UnityCatalogTable instance, but got: {type(table)}, {table}"
        )
    delta_lake_operator = DeltaLakeScanOperator(table_uri, storage_config=storage_config)

    handle = ScanOperatorHandle.from_python_scan_operator(delta_lake_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)
