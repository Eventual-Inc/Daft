from __future__ import annotations

import importlib
import logging
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlparse

from daft.dependencies import pa
from daft.logical.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.engine import Connection


logger = logging.getLogger(__name__)

SQLBackend = Literal["adbc", "connectorx", "sqlalchemy"]
_SQL_BACKENDS: tuple[str, ...] = ("adbc", "connectorx", "sqlalchemy")

# Maps a database URL dialect to the ADBC driver package (and module) that can read from it.
_ADBC_DRIVERS: dict[str, str] = {
    "postgres": "adbc_driver_postgresql",
    "postgresql": "adbc_driver_postgresql",
    "sqlite": "adbc_driver_sqlite",
}


class SQLConnection:
    def __init__(
        self,
        conn: str | Callable[[], Connection],
        driver: str,
        dialect: str,
        url: str,
        backend: SQLBackend | None = None,
    ) -> None:
        if backend is not None and backend not in _SQL_BACKENDS:
            raise ValueError(f"Invalid SQL backend: {backend!r}, must be one of {list(_SQL_BACKENDS)} or None.")
        if backend == "adbc" and dialect not in _ADBC_DRIVERS:
            raise ValueError(
                f"backend='adbc' does not support the {dialect!r} dialect. Supported dialects: {sorted(set(_ADBC_DRIVERS))}."
            )
        self.conn = conn
        self.dialect = dialect
        self.driver = driver
        self.url = url
        self.backend = backend

    def __repr__(self) -> str:
        # Deliberately omit the URL: secrets can appear anywhere in a
        # connection string (userinfo, query params, driver extras), so
        # the safest mitigation is to not echo it at all.
        return f"SQLConnection(dialect={self.dialect!r}, driver={self.driver!r}, backend={self.backend!r})"

    @classmethod
    def from_url(cls, url: str, backend: SQLBackend | None = None) -> SQLConnection:
        scheme = urlparse(url).scheme.strip().lower()
        if "+" in scheme:
            dialect, driver = scheme.split("+")
        else:
            dialect, driver = scheme, ""
        return cls(url, driver, dialect, url, backend=backend)

    @classmethod
    def from_connection_factory(
        cls, conn_factory: Callable[[], Connection], backend: SQLBackend | None = None
    ) -> SQLConnection:
        from sqlalchemy.engine import Connection

        if backend in ("adbc", "connectorx"):
            raise ValueError(
                f"backend={backend!r} requires a database URL. Use backend='sqlalchemy' or None with a connection factory."
            )

        try:
            with conn_factory() as connection:
                if not isinstance(connection, Connection):
                    raise ValueError(
                        f"Connection factory must return a SQLAlchemy connection object, got: {type(connection)}"
                    )
                dialect = connection.engine.dialect.name
                driver = connection.engine.driver
                url = connection.engine.url.render_as_string(hide_password=True)
            return cls(conn_factory, driver, dialect, url, backend=backend)
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
        if self.backend in ("adbc", "sqlalchemy"):
            return False
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

        if isinstance(self.conn, str) and self.dialect in connectorx_supported_dbs and self.driver == "":
            return True
        if self.backend == "connectorx":
            raise ValueError(
                f"backend='connectorx' does not support the {self.dialect!r} dialect with driver {self.driver!r}. "
                f"Supported dialects (without a driver suffix): {sorted(connectorx_supported_dbs)}."
            )
        return False

    def execute_sql_query(self, sql: str, schema: pa.Schema | None = None) -> pa.Table:
        if self.backend == "adbc":
            return self._execute_sql_query_with_adbc(sql, schema=schema)
        if schema is None and self._should_use_connectorx():
            return self._execute_sql_query_with_connectorx(sql)
        else:
            return self._execute_sql_query_with_sqlalchemy(sql, schema=schema)

    def connect_adbc(self) -> Any:
        """Open an ADBC DB-API connection for this URL.

        The caller is responsible for closing the returned connection.
        """
        assert isinstance(self.conn, str)
        package = _ADBC_DRIVERS[self.dialect]
        try:
            dbapi = importlib.import_module(f"{package}.dbapi")
        except ImportError as e:
            pip_name = package.replace("_", "-")
            raise ImportError(
                f"backend='adbc' needs the {pip_name} package to connect to {self.dialect!r} databases. "
                f"Install it with `pip install {pip_name}` or `pip install 'daft[sql-adbc]'`."
            ) from e
        return dbapi.connect(self._adbc_uri())

    def _adbc_uri(self) -> str:
        """Translate the SQLAlchemy-style URL into the form the ADBC driver expects."""
        assert isinstance(self.conn, str)
        rest = self.conn.split("://", 1)[1] if "://" in self.conn else ""
        if self.dialect == "sqlite":
            # sqlite:///relative.db -> relative.db, sqlite:////abs/path.db -> /abs/path.db, sqlite:// -> in-memory
            path = rest[1:] if rest.startswith("/") else rest
            return path or ":memory:"
        # Drop any SQLAlchemy driver suffix (e.g. postgresql+psycopg2://) since libpq only accepts postgresql://
        return f"postgresql://{rest}"

    def _execute_sql_query_with_adbc(self, sql: str, schema: pa.Schema | None = None) -> pa.Table:
        logger.info("Using ADBC to execute sql: %s", sql)
        try:
            with self.connect_adbc() as connection, connection.cursor() as cursor:
                cursor.execute(sql)
                table = cursor.fetch_arrow_table()
        except ImportError:
            raise
        except Exception as e:
            # See note in `_execute_sql_query_with_connectorx`: don't echo
            # back the connection URL.
            raise RuntimeError(f"Failed to execute sql: {sql}, error: {e}") from e

        if schema is not None:
            # Cast positionally into the requested schema, e.g. so that a MIN/MAX over an
            # all-NULL column comes back with the partition column's type.
            table = pa.Table.from_arrays(
                [table.column(i).cast(field.type) for i, field in enumerate(schema)], schema=schema
            )
        return table

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

    def _execute_sql_query_with_sqlalchemy(self, sql: str, schema: pa.Schema | None = None) -> pa.Table:
        from sqlalchemy import create_engine, text

        logger.info("Using sqlalchemy to execute sql: %s", sql)
        try:
            if isinstance(self.conn, str):
                with create_engine(self.conn).connect() as connection:
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
