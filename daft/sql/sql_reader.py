from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Callable
from urllib.parse import urlparse

import pyarrow as pa

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)


def get_db_dialect_and_driver_from_url(url: str) -> tuple[str, str]:
    scheme = urlparse(url).scheme.strip().lower()
    if "+" in scheme:
        dialect, driver = scheme.split("+")
    else:
        dialect, driver = scheme, ""
    return dialect, driver


class SQLReader:
    def __init__(
        self,
        sql: str,
        conn: str | Callable[[], Connection],
        limit: int | None = None,
        projection: list[str] | None = None,
        predicate: str | None = None,
    ) -> None:

        self.sql = sql
        self.conn = conn
        self.limit = limit
        self.projection = projection
        self.predicate = predicate

    def read(self) -> pa.Table:
        sql = self._construct_sql_query_with_limit()
        try:
            return self._execute_sql_query(sql)
        except RuntimeError as e:
            if self.limit is not None:
                warnings.warn(
                    f"Failed to execute the query with limit {self.limit}: {e}. Attempting to read the entire table."
                )
                return self._execute_sql_query(self._construct_sql_query())
            raise

    def _construct_sql_query(self) -> str:
        clauses = []
        if self.projection is not None:
            clauses.append(f"SELECT {', '.join(self.projection)}")
        else:
            clauses.append("SELECT *")

        clauses.append(f"FROM ({self.sql}) AS subquery")

        if self.predicate is not None:
            clauses.append(f"WHERE {self.predicate}")

        return "\n".join(clauses)

    def _construct_sql_query_with_limit(self) -> str:
        sql = self._construct_sql_query()
        return f"{sql}\nLIMIT {self.limit}" if self.limit is not None else sql

    def _execute_sql_query(self, sql: str) -> pa.Table:
        # Supported DBs extracted from here https://github.com/sfu-db/connector-x/tree/7b3147436b7e20b96691348143d605e2249d6119?tab=readme-ov-file#sources
        supported_dbs = {
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
            dialect, driver = get_db_dialect_and_driver_from_url(self.conn)

            # Use connectorx if the dialect is supported and driver is not specified
            if dialect in supported_dbs and driver == "":
                return self._execute_sql_query_with_connectorx(sql)
            else:
                return self._execute_sql_query_with_sqlalchemy(sql)

        else:
            return self._execute_sql_query_with_sqlalchemy(sql)

    def _execute_sql_query_with_connectorx(self, sql: str) -> pa.Table:
        import connectorx as cx

        assert isinstance(self.conn, str)
        logger.info(f"Using connectorx to execute sql: {sql}")
        try:
            table = cx.read_sql(conn=self.conn, query=sql, return_type="arrow")
            return table
        except Exception as e:
            raise RuntimeError(f"Failed to execute sql: {sql} with url: {self.conn}, error: {e}") from e

    def _execute_sql_query_with_sqlalchemy(self, sql: str) -> pa.Table:
        from sqlalchemy import create_engine, text

        logger.info(f"Using sqlalchemy to execute sql: {sql}")
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
