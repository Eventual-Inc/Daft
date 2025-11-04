"""SQL data sink module for Daft."""

from daft.io.sql.sql_data_sink import SQLDataSink, SQL_SINK_MODES, WriteSqlResult

__all__: tuple[str, ...] = (
    "SQL_SINK_MODES",
    "SQLDataSink",
    "WriteSqlResult",
)
