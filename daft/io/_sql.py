# isort: dont-add-import: from __future__ import annotations


from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import (
    NativeStorageConfig,
    PythonStorageConfig,
    ScanOperatorHandle,
    StorageConfig,
)
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder
from daft.sql.sql_scan import SQLScanOperator


def native_downloader_available(url: str) -> bool:
    # TODO: We should be able to support native downloads via ConnectorX for compatible databases
    return False


@PublicAPI
def read_sql(
    sql: str,
    url: str,
    use_native_downloader: bool = False,
    # schema_hints: Optional[Dict[str, DataType]] = None,
) -> DataFrame:
    """Creates a DataFrame from a SQL query

    Example:
        >>> def create_connection():
                return sqlite3.connect("example.db")
        >>> df = daft.read_sql("SELECT * FROM my_table", create_connection)

    Args:
        sql (str): SQL query to execute
        connection_factory (Callable[[], Connection]): A callable that returns a connection to the database.
        _multithreaded_io: Whether to use multithreading for IO threads. Setting this to False can be helpful in reducing
            the amount of system resources (number of connections and thread contention) when running in the Ray runner.
            Defaults to None, which will let Daft decide based on the runner it is currently using.

    returns:
        DataFrame: parsed DataFrame
    """

    io_config = context.get_context().daft_planning_config.default_io_config

    multithreaded_io = not context.get_context().is_ray_runner

    if use_native_downloader and native_downloader_available(url):
        storage_config = StorageConfig.native(NativeStorageConfig(multithreaded_io, io_config))
    else:
        storage_config = StorageConfig.python(PythonStorageConfig(io_config))

    sql_operator = SQLScanOperator(sql, url, storage_config=storage_config)

    handle = ScanOperatorHandle.from_python_scan_operator(sql_operator)
    builder = LogicalPlanBuilder.from_tabular_scan_with_scan_operator(scan_operator=handle)
    return DataFrame(builder)
