# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations
import os
from typing import TYPE_CHECKING, Optional, Union

from daft import context, runners
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.dependencies import unity_catalog
from daft.io.catalog import DataCatalogTable
from daft.logical.builder import LogicalPlanBuilder

if TYPE_CHECKING:
    from datetime import datetime

    import deltalake
    import pyarrow as pa

    from daft.unity_catalog import UnityCatalogTable


@PublicAPI
def read_deltalake(
    table: Union[str, DataCatalogTable, "UnityCatalogTable"],
    version: Optional[Union[int, str, "datetime"]] = None,
    io_config: Optional[IOConfig] = None,
    ignore_deletion_vectors: bool = False,
    _multithreaded_io: Optional[bool] = None,
) -> DataFrame:
    """Create a DataFrame from a Delta Lake table.

    Args:
        table: Either a URI for the Delta Lake table or a :class:`~daft.io.catalog.DataCatalogTable` instance
            referencing a table in a data catalog, such as AWS Glue Data Catalog or Databricks Unity Catalog.
        version (optional): If int is passed, read the table with specified version number. Otherwise if string or datetime,
            read the timestamp version of the table. Strings must be RFC 3339 and ISO 8601 date and time format.
            Datetimes are assumed to be UTC timezone unless specified. By default, read the latest version of the table.
        io_config (optional): A custom :class:`~daft.daft.IOConfig` to use when accessing Delta Lake object storage data. Defaults to None.
        ignore_deletion_vectors (optional): Whether to skip checking for deletion vectors when reading the table. Defaults to False.
        _multithreaded_io (optional): Whether to use multithreading for IO threads. Setting this to False can be helpful in reducing
            the amount of system resources (number of connections and thread contention) when running in the Ray runner.
            Defaults to None, which will let Daft decide based on the runner it is currently using.

    Returns:
        DataFrame: A DataFrame with the schema converted from the specified Delta Lake table.

    Note:
        This function requires the use of [deltalake](https://delta-io.github.io/delta-rs/), a Python library for interacting with Delta Lake.

    Examples:
        >>> df = daft.read_deltalake("some-table-uri")
        >>>
        >>> # Filters on this dataframe can now be pushed into
        >>> # the read operation from Delta Lake.
        >>> df = df.where(df["foo"] > 5)
        >>> df.show()
    """
    from daft.io.delta_lake.delta_lake_scan import DeltaLakeScanOperator

    # If running on Ray, we want to limit the amount of concurrency and requests being made.
    # This is because each Ray worker process receives its own pool of thread workers and connections
    multithreaded_io = (
        (runners.get_or_create_runner().name != "ray") if _multithreaded_io is None else _multithreaded_io
    )

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_config = StorageConfig(multithreaded_io, io_config)

    if isinstance(table, str):
        table_uri = os.path.expanduser(table)
    elif isinstance(table, DataCatalogTable):
        table_uri = table.table_uri(io_config)
    elif unity_catalog.module_available() and isinstance(table, unity_catalog.UnityCatalogTable):
        table_uri = table.table_uri

        # Override the storage_config with the one provided by Unity catalog
        recordbatch_io_config = table.io_config
        if recordbatch_io_config is not None:
            storage_config = StorageConfig(multithreaded_io, recordbatch_io_config)
    else:
        raise ValueError(
            f"table argument must be a table URI string, DataCatalogTable or UnityCatalogTable instance, but got: {type(table)}, {table}"
        )
    delta_lake_operator = DeltaLakeScanOperator(
        table_uri, storage_config=storage_config, version=version, ignore_deletion_vectors=ignore_deletion_vectors
    )

    handle = ScanOperatorHandle.from_python_scan_operator(delta_lake_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)


def delta_schema_to_pyarrow(schema: "deltalake.Schema") -> "pa.Schema":
    import deltalake
    from packaging.version import parse

    if parse(deltalake.__version__) < parse("1.0.0"):
        return schema.to_pyarrow()
    else:
        import pyarrow as pa

        return pa.schema(schema.to_arrow())
