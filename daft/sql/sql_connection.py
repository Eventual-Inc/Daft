from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Callable
from urllib.parse import urlparse

import pyarrow as pa

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)


class SQLConnection:
    def __init__(self, conn: str | Callable[[], Connection], driver: str, dialect: str, url: str) -> None:
        self.conn = conn
        self.dialect = dialect
        self.driver = driver
        self.url = url

    def __repr__(self) -> str:
        return f"SQLConnection(conn={self.conn})"

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
                url = connection.engine.url.render_as_string()
            return cls(conn_factory, driver, dialect, url)
        except Exception as e:
            raise ValueError(f"Unexpected error while calling the connection factory: {e}") from e

    def read(
        self, sql: str, projection: list[str] | None = None, predicate: str | None = None, limit: int | None = None
    ) -> pa.Table:
        sql = self._construct_sql_query(sql, projection, predicate, limit)
        try:
            return self._execute_sql_query(sql)
        except RuntimeError as e:
            if limit is not None:
                warnings.warn(
                    f"Failed to execute the query with limit {limit}: {e}. Attempting to read the entire table."
                )
                return self._execute_sql_query(self._construct_sql_query(sql, projection, predicate))
            raise

    def _construct_sql_query(
        self, sql: str, projection: list[str] | None = None, predicate: str | None = None, limit: int | None = None
    ) -> str:
        clauses = []
        if projection is not None:
            clauses.append(f"SELECT {', '.join(projection)}")
        else:
            clauses.append("SELECT *")

        clauses.append(f"FROM ({sql}) AS subquery")

        if predicate is not None:
            clauses.append(f"WHERE {predicate}")

        if limit is not None:
            clauses.append(f"LIMIT {limit}")

        return "\n".join(clauses)

    def _execute_sql_query(self, sql: str) -> pa.Table:
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
                return self._execute_sql_query_with_connectorx(sql)
            else:
                return self._execute_sql_query_with_sqlalchemy(sql)
        else:
            return self._execute_sql_query_with_sqlalchemy(sql)

    def _execute_sql_query_with_connectorx(self, sql: str) -> pa.Table:
        import connectorx as cx

        assert isinstance(self.conn, str)
        logger.info("Using connectorx to execute sql: %s", sql)
        try:
            table = cx.read_sql(conn=self.conn, query=sql, return_type="arrow")
            return table
        except Exception as e:
            raise RuntimeError(f"Failed to execute sql: {sql} with url: {self.conn}, error: {e}") from e

    def _execute_sql_query_with_sqlalchemy(self, sql: str) -> pa.Table:
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
            # TODO: Use type codes from cursor description to create pyarrow schema
            return pa.Table.from_pydict(pydict)
        except Exception as e:
            connection_str = self.conn if isinstance(self.conn, str) else self.conn.__name__
            raise RuntimeError(f"Failed to execute sql: {sql} from connection: {connection_str}, error: {e}") from e
