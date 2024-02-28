# isort: dont-add-import: from __future__ import annotations


from typing import Optional

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import PythonStorageConfig, ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder
from daft.sql.sql_scan import SQLScanOperator


@PublicAPI
def read_sql(sql: str, url: str, num_partitions: Optional[int] = None) -> DataFrame:
    """Creates a DataFrame from a SQL query

    Example:
        >>> df = daft.read_sql("SELECT * FROM my_table", "sqlite:///my_database.db")

    Args:
        sql (str): SQL query to execute
        url (str): URL to the database
        num_partitions (Optional[int]): Number of partitions to read the data into,
            defaults to None, which will lets Daft determine the number of partitions.

    returns:
        DataFrame: Dataframe containing the results of the query
    """

    io_config = context.get_context().daft_planning_config.default_io_config
    storage_config = StorageConfig.python(PythonStorageConfig(io_config))

    sql_operator = SQLScanOperator(sql, url, storage_config=storage_config, num_partitions=num_partitions)
    handle = ScanOperatorHandle.from_python_scan_operator(sql_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)

    if num_partitions is not None and num_partitions > 1 and not sql_operator._limit_and_offset_supported:
        return DataFrame(builder).into_partitions(num_partitions)
    else:
        return DataFrame(builder)
