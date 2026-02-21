# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from daft import Schema, context, from_pydict
from daft.api_annotations import PublicAPI
from daft.daft import ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.datatype import DataType
from daft.dependencies import np, pa
from daft.io.sink import DataSink, WriteResult
from daft.logical.builder import LogicalPlanBuilder
from daft.recordbatch.micropartition import MicroPartition
from daft.sql.sql_connection import SQLConnection
from daft.sql.sql_scan import PartitionBoundStrategy, SQLScanOperator

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection


@PublicAPI
def read_sql(
    sql: str,
    conn: Callable[[], "Connection"] | str,
    partition_col: str | None = None,
    num_partitions: int | None = None,
    partition_bound_strategy: str = "min-max",
    disable_pushdowns_to_sql: bool = False,
    infer_schema: bool = True,
    infer_schema_length: int = 10,
    schema: dict[str, DataType] | None = None,
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

    Note:
        1. **Supported dialects**:
            Daft uses [SQLGlot](https://sqlglot.com/sqlglot.html) to build and translate SQL queries between dialects. For a list of supported dialects, see [SQLGlot's dialect documentation](https://sqlglot.com/sqlglot/dialects.html).

        2. **Partitioning**:
            When `partition_col` is specified, the function partitions the query based on that column.
            You can define `num_partitions` or leave it to Daft to decide.
            Daft uses the `partition_bound_strategy` parameter to determine the partitioning strategy:
            - `min_max`: Daft calculates the minimum and maximum values of the specified column, then partitions the query using equal ranges between the minimum and maximum values.
            - `percentile`: Daft calculates the specified column's percentiles via a `PERCENTILE_DISC` function to determine partitions (e.g., for `num_partitions=3`, it uses the 33rd and 66th percentiles).

        3. **Execution**:
            Daft executes SQL queries using using [ConnectorX](https://sfu-db.github.io/connector-x/intro.html) or [SQLAlchemy](https://docs.sqlalchemy.org/en/20/orm/quickstart.html#create-an-engine),
            preferring ConnectorX unless a SQLAlchemy connection factory is specified or the database dialect is unsupported by ConnectorX.

        4. **Pushdowns**:
            Daft pushes down operations such as filtering, projections, and limits into the SQL query when possible.
            You can disable pushdowns by setting `disable_pushdowns_to_sql=True`, which will execute the SQL query as is.

    Examples:
        Read data from a SQL query and a database URL:

        >>> df = daft.read_sql("SELECT * FROM my_table", "sqlite:///my_database.db")

        Read data from a SQL query and a SQLAlchemy connection factory:

        >>> def create_conn():
        ...     return sqlalchemy.create_engine("sqlite:///my_database.db").connect()
        >>> df = daft.read_sql("SELECT * FROM my_table", create_conn)

        Read data from a SQL query and partition the data by a column:

        >>> df = daft.read_sql("SELECT * FROM my_table", "sqlite:///my_database.db", partition_col="id")

        Read data from a SQL query and partition the data into 3 partitions:

        >>> df = daft.read_sql(
        ...     "SELECT * FROM my_table", "sqlite:///my_database.db", partition_col="id", num_partitions=3
        ... )
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


@dataclass
class SQLDataSink(DataSink[dict[str, Any] | None]):
    """DataSink for writing DataFrames to SQL databases.

    This sink is used internally by :meth:`daft.DataFrame.write_sql` via
    :meth:`daft.DataFrame.write_sink` and is not part of the public API.

    The ``non_primitive_handling`` field controls how non-primitive columns
    (lists, structs, maps, tensors, images, embeddings, Python objects, etc.)
    are normalized before being written to SQL. See
    :meth:`daft.DataFrame.write_sql` for details.
    """

    table_name: str
    conn: str | Callable[[], "Connection"]
    write_mode: Literal["append", "overwrite", "fail"]
    chunk_size: int | None
    column_types: Any | None
    df_schema: Schema
    non_primitive_handling: Literal["bytes", "str", "error", "none"] | None = None

    def __post_init__(self) -> None:
        # Schema of the final result returned by ``finalize``.
        self._result_schema = Schema._from_field_name_and_types(
            [
                ("total_written_rows", DataType.int64()),
                ("total_written_bytes", DataType.int64()),
            ]
        )
        self._non_primitive_columns = self._detect_non_primitive_columns()

    def name(self) -> str:
        return "SQL Data Sink"

    def schema(self) -> Schema:
        return self._result_schema

    def _detect_non_primitive_columns(self) -> list[str]:
        """Return the column names in df_schema that are considered non-primitive for SQL writes."""
        non_primitive: list[str] = []
        for field in self.df_schema:
            dtype = field.dtype
            if (
                dtype.is_python()
                or dtype.is_list()
                or dtype.is_struct()
                or dtype.is_map()
                or dtype.is_tensor()
                or dtype.is_image()
                or dtype.is_embedding()
            ):
                non_primitive.append(field.name)
        return non_primitive

    @staticmethod
    def _coerce_value_to_str(value: Any) -> Any:
        if value is None:
            return None

        if isinstance(value, np.ndarray):
            to_serialize = value.tolist()
            try:
                return json.dumps(to_serialize)
            except TypeError:
                return str(to_serialize)

        if isinstance(value, (list, tuple, dict)):
            try:
                return json.dumps(value)
            except TypeError:
                return str(value)

        return str(value)

    @staticmethod
    def _coerce_value_to_bytes(value: Any) -> Any:
        if value is None:
            return None

        text = SQLDataSink._coerce_value_to_str(value)
        if text is None or isinstance(text, bytes):
            return text
        return str(text).encode("utf-8")

    def _normalize_non_primitive_values(self, pdf: Any) -> None:
        """Normalize values in non-primitive columns of a pandas DataFrame before writing.

        This mutates ``pdf`` in-place.
        """
        if not self._non_primitive_columns:
            return

        handling = self.non_primitive_handling

        if handling == "error":
            cols = ", ".join(self._non_primitive_columns)
            raise ValueError(
                "Cannot write non-primitive columns "
                f"{cols} to SQL when non_primitive_handling='error'. "
                "Drop or cast these columns, or choose 'str' or 'bytes' handling instead."
            )

        if handling is None or handling in ("none", "str"):
            mode = "str"
        elif handling == "bytes":
            mode = "bytes"
        else:
            raise ValueError(f"Invalid non_primitive_handling value: {handling!r}")

        if mode == "str":
            for col in self._non_primitive_columns:
                if col in pdf.columns:
                    pdf[col] = pdf[col].map(self._coerce_value_to_str)
        else:
            for col in self._non_primitive_columns:
                if col in pdf.columns:
                    pdf[col] = pdf[col].map(self._coerce_value_to_bytes)

    def start(self) -> None:
        """Driver-side initialization implementing first-block semantics.

        This method is responsible for applying the write_mode semantics once
        on the driver before distributed workers start appending data.
        """
        from sqlalchemy import create_engine, inspect

        engine = None
        connection: Connection | None = None

        try:
            if isinstance(self.conn, str):
                engine = create_engine(self.conn)
                connection = engine.connect()
            elif callable(self.conn):
                connection = self.conn()
            else:
                raise ValueError(f"Invalid conn type: {type(self.conn)}")

            inspector = inspect(connection)
            table_exists = inspector.has_table(self.table_name)

            # Build an empty pandas DataFrame that defines the schema for table creation on the driver side.
            pa_schema = self.df_schema.to_pyarrow_schema()
            empty_table = pa.Table.from_batches([], schema=pa_schema)
            empty_pdf = empty_table.to_pandas()

            if self.write_mode == "fail":
                if table_exists:
                    raise ValueError(f"Table {self.table_name!r} already exists, cannot write with write_mode='fail'.")
                # Create an empty table to establish schema.
                empty_pdf.to_sql(
                    self.table_name,
                    con=connection,
                    if_exists="fail",
                    index=False,
                    dtype=self.column_types,
                )
                connection.commit()
            elif self.write_mode == "overwrite":
                # Replace any existing table with an empty table that defines the schema.
                empty_pdf.to_sql(
                    self.table_name,
                    con=connection,
                    if_exists="replace",
                    index=False,
                    dtype=self.column_types,
                )
                connection.commit()
            else:  # append
                if not table_exists:
                    # Create the table once if it does not exist so workers can append safely.
                    empty_pdf.to_sql(
                        self.table_name,
                        con=connection,
                        if_exists="fail",
                        index=False,
                        dtype=self.column_types,
                    )
                    connection.commit()
        finally:
            if connection is not None:
                connection.close()
            if engine is not None:
                engine.dispose()

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[dict[str, Any] | None]]:
        """Write micropartitions to the SQL database.

        A new SQLAlchemy engine/connection is created per writer process to
        avoid serializing sockets across workers.
        """
        from sqlalchemy import create_engine

        engine = None
        connection: Connection | None = None

        try:
            if isinstance(self.conn, str):
                engine = create_engine(self.conn)
                connection = engine.connect()
            elif callable(self.conn):
                connection = self.conn()
            else:
                raise ValueError(f"Invalid conn type: {type(self.conn)}")

            for micropartition in micropartitions:
                pdf = micropartition.to_pandas()
                if len(pdf) == 0:
                    continue

                # Normalize any non-primitive values before handing them to the SQL driver.
                self._normalize_non_primitive_values(pdf)

                mp_size = micropartition.size_bytes()
                bytes_written: int = mp_size if mp_size is not None else 0
                rows_written: int = int(pdf.shape[0])

                pdf.to_sql(
                    self.table_name,
                    con=connection,
                    if_exists="append",
                    index=False,
                    chunksize=self.chunk_size,
                    dtype=self.column_types,
                )
                connection.commit()

                yield WriteResult(
                    result=None,
                    bytes_written=bytes_written,
                    rows_written=rows_written,
                )
        finally:
            if connection is not None:
                connection.close()
            if engine is not None:
                engine.dispose()

    def finalize(self, write_results: list[WriteResult[dict[str, Any] | None]]) -> MicroPartition:
        """Aggregate write statistics into a single MicroPartition."""
        total_written_rows = 0
        total_written_bytes = 0

        for write_result in write_results:
            total_written_rows += write_result.rows_written
            total_written_bytes += write_result.bytes_written

        return MicroPartition.from_pydict(
            {
                "total_written_rows": [total_written_rows],
                "total_written_bytes": [total_written_bytes],
            }
        )
