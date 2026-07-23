from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from daft.dependencies import pa
from daft.logical.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.engine import Connection, Engine


logger = logging.getLogger(__name__)


class SQLConnection:
    def __init__(self, conn: str | Callable[[], Connection], driver: str, dialect: str, url: str) -> None:
        self.conn = conn
        self.dialect = dialect
        self.driver = driver
        self.url = url
        self._engine = None
        self._engine_lock = threading.Lock()

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        # SQLAlchemy Engine contains connection pools and thread locks
        # that are not picklable. The engine will be lazily recreated
        # via _get_or_create_engine() after deserialization.
        state["_engine"] = None
        state["_engine_lock"] = None
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        if self._engine_lock is None:
            self._engine_lock = threading.Lock()

    def __repr__(self) -> str:
        # Deliberately omit the URL: secrets can appear anywhere in a
        # connection string (userinfo, query params, driver extras), so
        # the safest mitigation is to not echo it at all.
        return f"SQLConnection(dialect={self.dialect!r}, driver={self.driver!r})"

    @classmethod
    def from_url(cls, url: str) -> SQLConnection:
        scheme = urlparse(url).scheme.strip().lower()
        if "+" in scheme:
            dialect, driver = scheme.split("+")
        else:
            dialect, driver = scheme, ""
        return cls(url, driver, dialect, url)

    @classmethod
    def from_connection_factory(cls, conn_factory: Callable[[], Connection]) -> SQLConnection:
        from sqlalchemy.engine import Connection

        try:
            with conn_factory() as connection:
                if not isinstance(connection, Connection):
                    raise TypeError(
                        f"Connection factory must return a SQLAlchemy connection object, got: {type(connection)}"
                    )
                dialect = connection.engine.dialect.name
                driver = connection.engine.driver
                url = connection.engine.url.render_as_string(hide_password=True)
            return cls(conn_factory, driver, dialect, url)
        except Exception as e:
            raise ValueError(f"Unexpected error while calling the connection factory: {e}") from e

    def read_schema(self, sql: str, infer_schema_length: int) -> Schema:
        if self._should_use_connectorx():
            sql = self.construct_sql_query(sql, limit=0)
        else:
            sql = self.construct_sql_query(sql, limit=infer_schema_length)
        table = self.execute_sql_query(sql)
        schema = Schema.from_pyarrow_schema(table.schema)
        return schema

    def construct_sql_query(
        self,
        sql: str,
        projection: list[str] | None = None,
        predicate: str | None = None,
        limit: int | None = None,
        partition_bounds: tuple[str, str] | None = None,
    ) -> str:
        # If all options are None, just return the original sql
        if projection is None and predicate is None and limit is None and partition_bounds is None:
            return sql

        import sqlglot

        target_dialect = self.dialect
        # sqlglot does not support "postgresql" dialect, it only supports "postgres"
        if target_dialect == "postgresql":
            target_dialect = "postgres"
        # sqlglot does not recognize "mssql" as a dialect, it instead recognizes "tsql", which is the SQL dialect for Microsoft SQL Server
        elif target_dialect == "mssql":
            target_dialect = "tsql"
        # sqlglot does not recognize "awsathena", the dialect registered by PyAthena, SQLAlchemy driver for reading from AWS Athena. It only support "athena"
        elif target_dialect == "awsathena":
            target_dialect = "athena"

        if not any(target_dialect == supported_dialect.value for supported_dialect in sqlglot.Dialects):
            raise ValueError(
                f"Unsupported dialect: {target_dialect}, please refer to the documentation for supported dialects."
            )

        query = sqlglot.subquery(sql, "subquery", dialect=target_dialect)

        if projection is not None:
            query = query.select(*projection)
        else:
            query = query.select("*")

        if predicate is not None:
            query = query.where(predicate)

        if partition_bounds is not None:
            query = query.where(partition_bounds[0]).where(partition_bounds[1])

        if limit is not None:
            query = query.limit(limit)

        return query.sql(dialect=target_dialect)

    def _should_use_connectorx(self) -> bool:
        # Supported DBs extracted from here https://github.com/sfu-db/connector-x/tree/7b3147436b7e20b96691348143d605e2249d6119?tab=readme-ov-file#sources
        connectorx_supported_dbs = {
            "postgres",
            "postgresql",
            "mysql",
            "mssql",
            "oracle",
            "bigquery",
            "sqlite",
            "clickhouse",
            "redshift",
        }

        return isinstance(self.conn, str) and self.dialect in connectorx_supported_dbs and self.driver == ""

    def execute_sql_query(self, sql: str, schema: pa.Schema | None = None) -> pa.Table:
        if schema is None and self._should_use_connectorx():
            return self._execute_sql_query_with_connectorx(sql)
        else:
            return self._execute_sql_query_with_sqlalchemy(sql, schema=schema)

    def _execute_sql_query_with_connectorx(self, sql: str) -> pa.Table:
        import connectorx as cx

        assert isinstance(self.conn, str)
        logger.info("Using connectorx to execute sql: %s", sql)
        try:
            table = cx.read_sql(conn=self.conn, query=sql, return_type="arrow")
            return table
        except Exception as e:
            # The connection URL is deliberately omitted from the error message:
            # secrets can appear anywhere in it (userinfo, query params,
            # driver-specific extras), so dropping the URL is the only robust
            # mitigation. The caller knows which connection they passed in,
            # so the URL is redundant here.
            raise RuntimeError(f"Failed to execute sql: {sql}, error: {e}") from e

    def _get_or_create_engine(self) -> Engine | None:
        """Get or create a cached SQLAlchemy engine for string connection URLs."""
        # Fast path: engine already created (no lock needed for the read).
        if self._engine is not None:
            return self._engine
        # Slow path: serialize creation to avoid duplicate connection pools.
        with self._engine_lock:
            # Double-check after acquiring the lock.
            if self._engine is not None:
                return self._engine
            if not isinstance(self.conn, str):
                return None
            from sqlalchemy import create_engine

            url = self.conn
            # Auto-rewrite mysql:// to mysql+pymysql:// when pymysql is available
            # but mysqlclient is not. This avoids ModuleNotFoundError: No module named 'MySQLdb'
            if url.startswith("mysql://"):
                try:
                    import MySQLdb  # noqa: F401
                except ImportError:
                    try:
                        import pymysql  # type: ignore[import-untyped]  # noqa: F401

                        url = url.replace("mysql://", "mysql+pymysql://", 1)
                        logger.info("Rewrote mysql:// to mysql+pymysql:// (MySQLdb not available)")
                    except ImportError:
                        logger.warning(
                            "mysql:// URL detected but neither MySQLdb nor pymysql is installed. "
                            "Install pymysql to avoid ModuleNotFoundError: No module named 'MySQLdb'."
                        )

            self._engine = create_engine(url)
        return self._engine

    def _execute_sql_query_with_sqlalchemy(self, sql: str, schema: pa.Schema | None = None) -> pa.Table:
        from sqlalchemy import text

        logger.info("Using sqlalchemy to execute sql: %s", sql)
        try:
            if isinstance(self.conn, str):
                engine = self._get_or_create_engine()
                assert engine is not None  # guaranteed by isinstance(self.conn, str) guard
                with engine.connect() as connection:
                    result = connection.execute(text(sql))
                    rows = result.fetchall()
            else:
                with self.conn() as connection:
                    result = connection.execute(text(sql))
                    rows = result.fetchall()

            pydict = {column_name: [row[i] for row in rows] for i, column_name in enumerate(result.keys())}
            return pa.Table.from_pydict(pydict, schema=schema)
        except Exception as e:
            # See note in `_execute_sql_query_with_connectorx`: don't echo
            # back the connection URL.
            raise RuntimeError(f"Failed to execute sql: {sql}, error: {e}") from e
