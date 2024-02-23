# isort: dont-add-import: from __future__ import annotations


from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import PythonStorageConfig, ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder
from daft.sql.sql_scan import SQLScanOperator


@PublicAPI
def read_sql(
    sql: str,
    url: str,
) -> DataFrame:
    """Creates a DataFrame from a SQL query

    Example:
        >>> df = daft.read_sql("SELECT * FROM my_table", "sqlite:///my_database.db")

    Args:
        sql (str): SQL query to execute
        url (str): URL to the database

    returns:
        DataFrame: parsed DataFrame
    """

    io_config = context.get_context().daft_planning_config.default_io_config
    storage_config = StorageConfig.python(PythonStorageConfig(io_config))

    sql_operator = SQLScanOperator(sql, url, storage_config=storage_config)
    handle = ScanOperatorHandle.from_python_scan_operator(sql_operator)
    builder = LogicalPlanBuilder.from_tabular_scan_with_scan_operator(scan_operator=handle)
    return DataFrame(builder)
