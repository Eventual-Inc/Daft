# isort: dont-add-import: from __future__ import annotations


from typing import Optional

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import PythonStorageConfig, ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder
from daft.sql.sql_scan import SQLScanOperator


@PublicAPI
def read_sql(
    sql: str, url: str, partition_col: Optional[str] = None, num_partitions: Optional[int] = None
) -> DataFrame:
    """Creates a DataFrame from a SQL query.

    Example:
        >>> df = daft.read_sql("SELECT * FROM my_table", "sqlite:///my_database.db")

    .. NOTE::
        If partition_col is specified, this function will partition the query by the specified column. You may specify the number of partitions, or let Daft determine the number of partitions.
        Daft will attempt to partition the query on the percentiles of the specified column, and will attempt to balance the number of rows in each partition.
        If the database does not support the necessary SQL syntax to partition the query, Daft will partition the query via ranges between the min and max values of the specified column.

    Args:
        sql (str): SQL query to execute
        url (str): URL to the database
        partition_col (Optional[str]): Column to partition the data by, defaults to None
        num_partitions (Optional[int]): Number of partitions to read the data into,
            defaults to None, which will lets Daft determine the number of partitions.

    Returns:
        DataFrame: Dataframe containing the results of the query
    """

    if num_partitions is not None and partition_col is None:
        raise ValueError("Failed to execute sql: partition_col must be specified when num_partitions is specified")

    io_config = context.get_context().daft_planning_config.default_io_config
    storage_config = StorageConfig.python(PythonStorageConfig(io_config))

    sql_operator = SQLScanOperator(
        sql,
        url,
        storage_config,
        partition_col=partition_col,
        num_partitions=num_partitions,
    )
    handle = ScanOperatorHandle.from_python_scan_operator(sql_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)

    return DataFrame(builder)
