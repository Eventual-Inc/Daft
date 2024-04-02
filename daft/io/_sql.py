# isort: dont-add-import: from __future__ import annotations


from typing import TYPE_CHECKING, Callable, Optional, Union

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import PythonStorageConfig, ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder
from daft.sql.sql_scan import SQLScanOperator

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


@PublicAPI
def read_sql(
    sql: str,
    conn: Union[Callable[[], "Connection"], str],
    partition_col: Optional[str] = None,
    num_partitions: Optional[int] = None,
) -> DataFrame:
    """Creates a DataFrame from the results of a SQL query.

    Args:
        sql (str): SQL query to execute
        conn (Union[Callable[[], Connection], str]): SQLAlchemy connection factory or database URL
        partition_col (Optional[str]): Column to partition the data by, defaults to None
        num_partitions (Optional[int]): Number of partitions to read the data into,
            defaults to None, which will lets Daft determine the number of partitions.

    Returns:
        DataFrame: Dataframe containing the results of the query

    .. NOTE::
        #. Partitioning:
            When `partition_col` is specified, the function partitions the query based on that column.
            You can define `num_partitions` or leave it to Daft to decide.
            Daft calculates the specified column's percentiles to determine partitions (e.g., for `num_partitions=3`, it uses the 33rd and 66th percentiles).
            If the database or column type lacks percentile calculation support, Daft partitions the query using equal ranges between the column's minimum and maximum values.

        #. Execution:
            Daft executes SQL queries using using `ConnectorX <https://sfu-db.github.io/connector-x/intro.html>`_ or `SQLAlchemy <https://docs.sqlalchemy.org/en/20/orm/quickstart.html#create-an-engine>`_,
            preferring ConnectorX unless a SQLAlchemy connection factory is specified or the database dialect is unsupported by ConnectorX.

    Example:
        Read data from a SQL query and a database URL:

        >>> df = daft.read_sql("SELECT * FROM my_table", "sqlite:///my_database.db")

        Read data from a SQL query and a SQLAlchemy connection factory:

        >>> def create_conn():
        ...     return sqlalchemy.create_engine("sqlite:///my_database.db").connect()
        >>> df = daft.read_sql("SELECT * FROM my_table", create_conn)

        Read data from a SQL query and partition the data by a column:

        >>> df = daft.read_sql(
        ...     "SELECT * FROM my_table",
        ...     "sqlite:///my_database.db",
        ...     partition_col="id"
        ... )

        Read data from a SQL query and partition the data into 3 partitions:

        >>> df = daft.read_sql(
        ...     "SELECT * FROM my_table",
        ...     "sqlite:///my_database.db",
        ...     partition_col="id",
        ...     num_partitions=3
        ... )
    """

    if num_partitions is not None and partition_col is None:
        raise ValueError("Failed to execute sql: partition_col must be specified when num_partitions is specified")

    io_config = context.get_context().daft_planning_config.default_io_config
    storage_config = StorageConfig.python(PythonStorageConfig(io_config))

    sql_operator = SQLScanOperator(
        sql,
        conn,
        storage_config,
        partition_col=partition_col,
        num_partitions=num_partitions,
    )
    handle = ScanOperatorHandle.from_python_scan_operator(sql_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)

    return DataFrame(builder)
