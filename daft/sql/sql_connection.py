from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from urllib.parse import parse_qsl, urlencode, urlparse

from daft.dependencies import pa
from daft.logical.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.engine import Connection


logger = logging.getLogger(__name__)


# Query-parameter names whose values must be redacted. Substring match,
# case-insensitive. Covers password / OAuth token / API key / key passphrase
# styles used by common SQL drivers (Trino JWT auth, Snowflake key auth,
# Databricks PATs, generic API keys, etc.).
_SENSITIVE_PARAM_KEYWORDS = (
    "password",
    "pwd",
    "secret",
    "token",
    "passphrase",
    "apikey",
    "api_key",
    "credential",
)


def _is_sensitive_param(name: str) -> bool:
    lower = name.casefold()
    return any(keyword in lower for keyword in _SENSITIVE_PARAM_KEYWORDS)


def _redact_url(url: str) -> str:
    # Prefer SQLAlchemy's URL parser: it handles unescaped URL-special
    # characters in passwords (e.g. '#', '/', '?') that urllib.parse silently
    # mis-parses, dropping the password into the fragment/path/query and
    # making it look "absent" — which would otherwise cause this function to
    # return the URL unchanged and leak credentials.
    try:
        from sqlalchemy.engine import make_url

        sa_url = make_url(url)
        sensitive_keys = [k for k in sa_url.query if _is_sensitive_param(k)]
        if sa_url.password is None and not sensitive_keys:
            return url
        if sensitive_keys:
            new_query = {k: ("***" if _is_sensitive_param(k) else v) for k, v in sa_url.query.items()}
            sa_url = sa_url.set(query=new_query)
        # SQLAlchemy percent-encodes '*' to '%2A' in query values; undo that
        # for the redaction placeholder so the rendered URL stays readable.
        return sa_url.render_as_string(hide_password=True).replace("%2A%2A%2A", "***")
    except Exception:
        pass

    # Fallback for environments without SQLAlchemy (the connectorx-only path).
    try:
        parsed = urlparse(url)
        query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
        any_sensitive_query = any(_is_sensitive_param(k) for k, _ in query_pairs)
        redacted_query = (
            urlencode([(k, "***" if _is_sensitive_param(k) else v) for k, v in query_pairs])
            if any_sensitive_query
            else parsed.query
        )

        if parsed.password is not None:
            userinfo = f"{parsed.username}:***" if parsed.username else "***"
            host = parsed.hostname or ""
            if parsed.port is not None:
                host = f"{host}:{parsed.port}"
            return parsed._replace(netloc=f"{userinfo}@{host}", query=redacted_query).geturl()
        if any_sensitive_query:
            return parsed._replace(query=redacted_query).geturl()
        # urlparse missed the password — but if there's an "@" before the
        # path/query/fragment, it's almost certainly userinfo with unescaped
        # special chars in the password. Over-redact to be safe.
        if "://" in url:
            after_scheme = url.split("://", 1)[1]
            authority = after_scheme
            for delim in ("/", "?", "#"):
                idx = authority.find(delim)
                if idx != -1:
                    authority = authority[:idx]
            if "@" in authority:
                return "<redacted>"
        return url
    except Exception:
        return "<redacted>"


class SQLConnection:
    def __init__(self, conn: str | Callable[[], Connection], driver: str, dialect: str, url: str) -> None:
        self.conn = conn
        self.dialect = dialect
        self.driver = driver
        self.url = url

    def __repr__(self) -> str:
        conn = _redact_url(self.conn) if isinstance(self.conn, str) else self.conn
        return f"SQLConnection(conn={conn})"

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
                    raise ValueError(
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

        if isinstance(self.conn, str):
            if self.dialect in connectorx_supported_dbs and self.driver == "":
                return True
        return False

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
            raise RuntimeError(f"Failed to execute sql: {sql} with url: {_redact_url(self.conn)}, error: {e}") from e

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
            connection_str = _redact_url(self.conn) if isinstance(self.conn, str) else self.conn.__name__
            raise RuntimeError(f"Failed to execute sql: {sql} from connection: {connection_str}, error: {e}") from e
