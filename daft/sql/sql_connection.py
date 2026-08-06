from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from daft.dependencies import pa
from daft.logical.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.engine import Connection


logger = logging.getLogger(__name__)


class SQLConnection:
    def __init__(self, conn: str | Callable[[], Connection], driver: str, dialect: str, url: str) -> None:
        self.conn = conn
        self.dialect = dialect
        self.driver = driver
        self.url = url

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
            # ConnectorX can infer column types from LIMIT 0 metadata
            # without transferring data.
            schema_sql = self.construct_sql_query(sql, limit=0)
            table = self.execute_sql_query(schema_sql)
            # When ConnectorX fails and falls back to SQLAlchemy, LIMIT 0
            # produces null column types because there is no data to infer
            # from.  Retry with a proper row count, skipping ConnectorX to
            # avoid a second COM_STMT_PREPARE failure + warning log.
            if any(dt == pa.null() for dt in table.schema.types):
                schema_sql = self.construct_sql_query(sql, limit=infer_schema_length)
                table = self._execute_sql_query_with_sqlalchemy(schema_sql, _conn=self._sa_ready_url())
        else:
            schema_sql = self.construct_sql_query(sql, limit=infer_schema_length)
            table = self.execute_sql_query(schema_sql)
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

    def _sa_ready_url(self) -> str:
        """Return a SQLAlchemy-ready URL string.

        Bare ``mysql://`` URLs default to the mysqldb driver which requires
        ``mysqlclient`` (not included in Daft's sql extra).  Rewrite them to
        ``mysql+pymysql://`` so PyMySQL (already a dev dependency and widely
        available) is used instead.

        Must only be called when ``self.conn`` is a string (i.e., when the
        ConnectorX path or its fallback is relevant).
        """
        assert isinstance(self.conn, str), "_sa_ready_url() requires a string connection"
        if self.conn.startswith("mysql://"):
            return self.conn.replace("mysql://", "mysql+pymysql://", 1)
        return self.conn

    def _execute_sql_query_with_connectorx(self, sql: str) -> pa.Table:
        import connectorx as cx

        assert isinstance(self.conn, str)
        logger.info("Using connectorx to execute sql: %s", sql)
        try:
            table = cx.read_sql(conn=self.conn, query=sql, return_type="arrow")
            return table
        except Exception as e:
            # Fallback to SQLAlchemy only for COM_STMT_PREPARE failures.
            # MySQL-compatible databases like Doris and StarRocks do not support
            # the binary prepared-statement protocol that ConnectorX uses.
            # Connection-level errors (auth, network, DNS) should surface immediately.
            if "COM_STMT_PREPARE" not in str(e):
                raise RuntimeError(f"Failed to execute sql: {sql}, error: {type(e).__name__}") from e
            # COM_STMT_PREPARE failures may include the connection URL
            # in their error message — sanitize before logging.
            logger.warning(
                "ConnectorX COM_STMT_PREPARE failed, falling back to SQLAlchemy: %s",
                str(e).replace(self.conn, "<redacted>") if isinstance(self.conn, str) else e,
            )
            fallback_conn = self._sa_ready_url()
            try:
                return self._execute_sql_query_with_sqlalchemy(sql, _conn=fallback_conn)
            except Exception as sqlalchemy_error:
                # Sanitize exception messages: strip the connection URL to
                # prevent credential leakage (secrets can appear anywhere in a
                # URL – userinfo, query params, driver extras).  We still expose
                # the exception type and the sanitised message so callers can
                # diagnose which backend failed and why.
                if isinstance(self.conn, str):
                    cx_msg = str(e).replace(self.conn, "<redacted>")
                    sa_msg = str(sqlalchemy_error).replace(self.conn, "<redacted>")
                    # Also sanitize the rewritten URL if it differs.
                    if fallback_conn != self.conn:
                        sa_msg = sa_msg.replace(fallback_conn, "<redacted>")
                else:
                    cx_msg = str(e)
                    sa_msg = str(sqlalchemy_error)
                raise RuntimeError(
                    f"Failed to execute sql with both ConnectorX ({type(e).__name__}: {cx_msg}) "
                    f"and SQLAlchemy ({type(sqlalchemy_error).__name__}: {sa_msg})"
                ) from e

    def _execute_sql_query_with_sqlalchemy(
        self, sql: str, schema: pa.Schema | None = None, *, _conn: str | None = None
    ) -> pa.Table:
        from sqlalchemy import create_engine, text

        logger.info("Using sqlalchemy to execute sql: %s", sql)
        try:
            if _conn is not None or isinstance(self.conn, str):
                conn_url = _conn if _conn is not None else self.conn
                with create_engine(conn_url).connect() as connection:
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
