"""SQL Data Sink for writing DataFrames to SQL databases."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Literal, TypedDict, Union

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    Time,
    create_engine,
    inspect,
    text,
)

from daft.datatype import DataType
from daft.dependencies import pa
from daft.io import DataSink
from daft.io.sink import WriteResult
from daft.recordbatch.micropartition import MicroPartition
from daft.schema import Schema
from daft.sql.sql_connection import SQLConnection

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.engine import Connection
    from sqlalchemy.types import TypeEngine


__all__: tuple[str, ...] = (
    "SQL_SINK_MODES",
    "SQLDataSink",
    "WriteSqlResult",
)

SQL_SINK_MODES = Literal["create", "append", "replace"]
"""The supported write modes for the SQLDataSink.

The supported modes are:
    - "create": This creates a new table in the database. It's an error if the table already exists.
    - "append": Inserts new rows into an existing table. It's an error if the table does not exist.
    - "replace": Like "create", but it deletes the table if it already exists.
"""


class WriteSqlResult(TypedDict):
    table_name: str


class SQLDataSink(DataSink[WriteSqlResult]):
    """DataSink for writing data to SQL databases.

    Supports PostgreSQL, SQLite, MySQL, and other SQLAlchemy-compatible databases.
    """

    def __init__(
        self,
        *,
        table_name: str,
        connection: Union[str, Callable[[], Connection]],  # noqa: UP007
        mode: SQL_SINK_MODES,
        schema: Schema,
    ) -> None:
        """Initialize SQLDataSink.

        Args:
            table_name: Name of the target SQL table.
            connection: Connection string (URL) or factory callable that returns a connection.
            mode: How this sink should write into the database.
            schema: The schema for the Daft DataFrame that we're writing.
        """
        self._table_name = table_name
        self._connection = connection
        self._mode = mode
        self._df_schema = schema

        # Create SQLConnection for dialect detection and schema handling
        if isinstance(connection, str):
            self._sql_connection = SQLConnection.from_url(connection)
        else:
            self._sql_connection = SQLConnection.from_connection_factory(connection)

        # Define the schema for the result of the write operation
        self._result_schema = Schema._from_field_name_and_types(
            [
                ("rows_written", DataType.uint64()),
                ("bytes_written", DataType.uint64()),
                ("table_name", DataType.string()),
            ]
        )

    def name(self) -> str:
        """Return the name of this sink."""
        return "SQLDataSink"

    def schema(self) -> Schema:
        """Return the schema of the result micropartition."""
        return self._result_schema

    def start(self) -> None:
        """Prepare the database table for writing."""
        conn = self._get_connection()
        try:
            if self._mode == "create":
                create_table(self._table_name, conn, self._df_schema)
            elif self._mode == "append":
                ensure_table_exists(self._table_name, conn, self._df_schema)
            elif self._mode == "replace":
                drop_table_if_exists(self._table_name, conn)
                create_table(self._table_name, conn, self._df_schema)
            else:
                raise ValueError(f"Unrecognized mode: '{self._mode}'. Expecting one of {SQL_SINK_MODES}")
        finally:
            conn.close()

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[WriteSqlResult]]:
        """Write micropartitions to the SQL database using PyArrow tables.

        Args:
            micropartitions: Iterator of micropartitions to write

        Yields:
            WriteResult: Result containing rows/bytes written metadata
        """
        conn = self._get_connection()

        metadata = MetaData()
        metadata.reflect(bind=conn)

        result = WriteSqlResult(table_name=self._table_name)

        try:
            for micropartition in micropartitions:
                # Convert MicroPartition to PyArrow table
                arrow_table = micropartition.to_arrow()
                rows_written = arrow_table.num_rows
                bytes_written = arrow_table.nbytes

                table = create_table_from_arrow(self._table_name, conn, arrow_table, metadata)

                insert_arrow_table(conn, table, arrow_table)

                yield WriteResult(
                    result=result,
                    bytes_written=bytes_written,
                    rows_written=rows_written,
                )

            # Commit the transaction, writing all micropartition updates into the database
            conn.commit()

        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"Failed to write to SQL table '{self._table_name}': {e}") from e
        finally:
            conn.close()

    def _get_connection(self) -> Connection:
        """Get a database connection."""
        if isinstance(self._connection, str):
            return create_engine(self._connection).connect()
        else:
            return self._connection()

    def finalize(self, write_results: list[WriteResult[WriteSqlResult]]) -> MicroPartition:
        """Finalize the write operation and return summary statistics.

        Args:
            write_results: List of results from write() calls

        Returns:
            MicroPartition: Summary of the write operation
        """
        total_rows = 0
        total_bytes = 0

        for write_result in write_results:
            total_rows += write_result.rows_written
            total_bytes += write_result.bytes_written

        # Create result micropartition
        tbl = MicroPartition.from_pydict(
            {
                "rows_written": pa.array([total_rows], pa.uint64()),
                "bytes_written": pa.array([total_bytes], pa.uint64()),
                "table_name": pa.array([self._table_name], pa.string()),
            }
        )
        return tbl


def create_table(table_name: str, conn: Connection, df_schema: Schema) -> None:
    """Create a new table in the database."""
    inspector = inspect(conn)
    if table_name in inspector.get_table_names():
        raise ValueError(
            f"Table '{table_name}' already exists! Use mode='append' or mode='replace' instead of mode='create'"
        )

    # Create the table
    create_table_sql = f"CREATE TABLE {table_name} ({', '.join([f'{col.name} {col.type}' for col in df_schema])})"
    conn.execute(text(create_table_sql))
    conn.commit()


def ensure_table_exists(table_name: str, conn: Connection, df_schema: Schema) -> None:
    """Ensure that a table exists in the database."""
    inspector = inspect(conn)
    if table_name not in inspector.get_table_names():
        raise ValueError(f"Table '{table_name}' does not exist!")

    # The table must have the exact same columns (name + type) of the DataFrame that we're writing.
    existing_columns = inspector.get_columns(table_name)

    # First, we check that all existing table columns match the DataFrame schema.
    for existing_column in existing_columns:
        name = existing_column["name"]
        if name not in df_schema.names:
            raise ValueError(
                f"Extra column '{name}' exists in the table but it does not does not exist in DataFrame schema!"
            )

        df_col = df_schema[name]
        df_type_sql = arrow_type_to_sqlalchemy_type(df_col.dtype.to_arrow_type())

        col_type_sql = existing_column["type"]

        if df_type_sql != col_type_sql:
            raise ValueError(
                f"Column '{name}' type mismatch! DataFrame has {df_type_sql} (Daft: {df_col.dtype}) but "
                f"existing table's ({table_name}) column type is {col_type_sql}."
            )

    # Second, we check that we have the same number of columns.
    if len(existing_columns) != len(df_schema.names):
        raise ValueError(
            f"DataFrame has {len(df_schema.names)} columns but table {table_name} has {len(existing_columns)} columns."
        )

    # All columns match and there are no missing columns!


def drop_table_if_exists(table_name: str, conn: Connection) -> None:
    """Drop the table if it exists."""
    inspector = inspect(conn)
    if table_name in inspector.get_table_names():
        conn.execute(text(f"DROP TABLE {table_name}"))
        conn.commit()


def create_table_from_arrow(table_name: str, conn: Connection, arrow_table: pa.Table, metadata: MetaData) -> Table:
    """Create a SQL table from an Arrow table schema."""
    columns = arrow_schema_to_sqlalchemy_columns(arrow_table.schema)
    table = Table(table_name, metadata, *columns)
    metadata.create_all(conn)
    return table


def arrow_schema_to_sqlalchemy_columns(arrow_schema: pa.Schema) -> list[Column]:
    """Convert Arrow schema to SQLAlchemy Column objects."""
    columns = []
    for field in arrow_schema:
        col_type = arrow_type_to_sqlalchemy_type(field.type)
        col = Column(field.name, col_type, nullable=field.nullable)
        columns.append(col)
    return columns


def arrow_type_to_sqlalchemy_type(arrow_type: pa.DataType) -> TypeEngine[Any]:
    """Convert Arrow type to SQLAlchemy type."""
    if pa.types.is_int8(arrow_type):
        return Integer()
    elif pa.types.is_int16(arrow_type):
        return Integer()
    elif pa.types.is_int32(arrow_type):
        return Integer()
    elif pa.types.is_int64(arrow_type):
        return BigInteger()
    elif pa.types.is_uint8(arrow_type):
        return Integer()
    elif pa.types.is_uint16(arrow_type):
        return Integer()
    elif pa.types.is_uint32(arrow_type):
        return BigInteger()
    elif pa.types.is_uint64(arrow_type):
        return BigInteger()
    elif pa.types.is_floating(arrow_type):
        return Float()
    elif pa.types.is_boolean(arrow_type):
        return Boolean()
    elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return String()
    elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return LargeBinary()
    elif pa.types.is_date(arrow_type):
        return Date()
    elif pa.types.is_time(arrow_type):
        return Time()
    elif pa.types.is_timestamp(arrow_type):
        return DateTime()
    elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return JSON()
    elif pa.types.is_struct(arrow_type):
        return JSON()
    else:
        # Default to String for unknown types
        return String()


def insert_arrow_table(conn: Connection, table: Table, arrow_table: pa.Table) -> None:
    """Insert Arrow table data into SQL table.

    Uses PyArrow's to_pylist() to convert to Python objects for SQLAlchemy insertion.
    """
    if arrow_table.num_rows == 0:
        return

    # Convert Arrow table to list of dicts for insertion
    data = arrow_table.to_pylist()

    # Insert using SQLAlchemy insert statement
    insert_stmt = table.insert()
    conn.execute(insert_stmt, data)
