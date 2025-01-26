# isort: dont-add-import: from __future__ import annotations


from typing import TYPE_CHECKING, Callable, Dict, Optional, Union

from daft import context, from_pydict
from daft.api_annotations import PublicAPI
from daft.daft import ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.datatype import DataType
from daft.logical.builder import LogicalPlanBuilder
from daft.sql.sql_connection import SQLConnection
from daft.sql.sql_scan import PartitionBoundStrategy, SQLScanOperator

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


@PublicAPI
def read_sql(
    sql: str,
    conn: Union[Callable[[], "Connection"], str],
    partition_col: Optional[str] = None,
    num_partitions: Optional[int] = None,
    partition_bound_strategy: str = "min-max",
    disable_pushdowns_to_sql: bool = False,
    infer_schema: bool = True,
    infer_schema_length: int = 10,
    schema: Optional[Dict[str, DataType]] = None,
) -> DataFrame:
    """Create a DataFrame from the results of a SQL query.

    Args:
        sql (str): SQL query to execute
        conn (Union[Callable[[], Connection], str]): SQLAlchemy connection factory or database URL
        partition_col (Optional[str]): Column to partition the data by, defaults to None
        num_partitions (Optional[int]): Number of partitions to read the data into,
            defaults to None, which will lets Daft determine the number of partitions.
            If specified, `partition_col` must also be specified.
        partition_bound_strategy (str): Strategy to determine partition bounds, either "min-max" or "percentile", defaults to "min-max"
        disable_pushdowns_to_sql (bool): Whether to disable pushdowns to the SQL query, defaults to False
        infer_schema (bool): Whether to turn on schema inference, defaults to True. If set to False, the schema parameter must be provided.
        infer_schema_length (int): The number of rows to scan when inferring the schema, defaults to 10. If infer_schema is False, this parameter is ignored. Note that if Daft is able to use ConnectorX to infer the schema, this parameter is ignored as ConnectorX is an Arrow backed driver.
        schema (Optional[Dict[str, DataType]]): A mapping of column names to datatypes. If infer_schema is False, this schema is used as the definitive schema for the data, otherwise it is used as a schema hint that is applied after the schema is inferred.
            This can be useful if the types can be more precisely determined than what the inference can provide (e.g., if a column can be declared as a fixed-sized list rather than a list).

    Returns:
        DataFrame: Dataframe containing the results of the query

    .. NOTE::
        #. Supported dialects:
            Daft uses `SQLGlot <https://sqlglot.com/sqlglot.html>`_ to build and translate SQL queries between dialects. For a list of supported dialects, see `SQLGlot's dialect documentation <https://sqlglot.com/sqlglot/dialects.html>`_.

        #. Partitioning:
            When `partition_col` is specified, the function partitions the query based on that column.
            You can define `num_partitions` or leave it to Daft to decide.
            Daft uses the `partition_bound_strategy` parameter to determine the partitioning strategy:
            - `min_max`: Daft calculates the minimum and maximum values of the specified column, then partitions the query using equal ranges between the minimum and maximum values.
            - `percentile`: Daft calculates the specified column's percentiles via a `PERCENTILE_DISC` function to determine partitions (e.g., for `num_partitions=3`, it uses the 33rd and 66th percentiles).

        #. Execution:
            Daft executes SQL queries using using `ConnectorX <https://sfu-db.github.io/connector-x/intro.html>`_ or `SQLAlchemy <https://docs.sqlalchemy.org/en/20/orm/quickstart.html#create-an-engine>`_,
            preferring ConnectorX unless a SQLAlchemy connection factory is specified or the database dialect is unsupported by ConnectorX.

        #. Pushdowns:
            Daft pushes down operations such as filtering, projections, and limits into the SQL query when possible.
            You can disable pushdowns by setting `disable_pushdowns_to_sql=True`, which will execute the SQL query as is.

    Example:
        Read data from a SQL query and a database URL:

        >>> df = daft.read_sql("SELECT * FROM my_table", "sqlite:///my_database.db")

        Read data from a SQL query and a SQLAlchemy connection factory:

        >>> def create_conn():
        ...     return sqlalchemy.create_engine("sqlite:///my_database.db").connect()
        >>> df = daft.read_sql("SELECT * FROM my_table", create_conn)

        Read data from a SQL query and partition the data by a column:

        >>> df = daft.read_sql("SELECT * FROM my_table", "sqlite:///my_database.db", partition_col="id")

        Read data from a SQL query and partition the data into 3 partitions:

        >>> df = daft.read_sql("SELECT * FROM my_table", "sqlite:///my_database.db", partition_col="id", num_partitions=3)
    """
    if num_partitions is not None and partition_col is None:
        raise ValueError("Failed to execute sql: partition_col must be specified when num_partitions is specified")

    if not infer_schema and schema is None:
        raise ValueError(
            "Cannot read DataFrame with infer_schema=False and schema=None, please provide a schema or set infer_schema=True"
        )

    io_config = context.get_context().daft_planning_config.default_io_config
    storage_config = StorageConfig(True, io_config)

    sql_conn = SQLConnection.from_url(conn) if isinstance(conn, str) else SQLConnection.from_connection_factory(conn)
    sql_operator = SQLScanOperator(
        sql,
        sql_conn,
        storage_config,
        disable_pushdowns_to_sql,
        infer_schema,
        infer_schema_length,
        schema,
        partition_col=partition_col,
        num_partitions=num_partitions,
        partition_bound_strategy=PartitionBoundStrategy.from_str(partition_bound_strategy),
    )
    handle = ScanOperatorHandle.from_python_scan_operator(sql_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)

    return DataFrame(builder)


@PublicAPI
def sql(sql: str) -> DataFrame:
    """Create a DataFrame from an SQL query.

    EXPERIMENTAL: This features is early in development and will change.

    Args:
        sql (str): SQL query to execute

    Returns:
        DataFrame: Dataframe containing the results of the query
    """
    return from_pydict({})
