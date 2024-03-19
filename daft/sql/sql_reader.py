from __future__ import annotations

import logging
import warnings
from urllib.parse import urlparse

import pyarrow as pa

logger = logging.getLogger(__name__)


class SQLReader:
    def __init__(
        self,
        sql: str,
        url: str,
        limit: int | None = None,
        projection: list[str] | None = None,
        predicate: str | None = None,
    ) -> None:

        self.sql = sql
        self.url = url
        self.limit = limit
        self.projection = projection
        self.predicate = predicate

    def read(self) -> pa.Table:
        try:
            sql = self._construct_sql_query(apply_limit=True)
            return self._execute_sql_query(sql)
        except RuntimeError:
            warnings.warn("Failed to execute the query with a limit, attempting to read the entire table.")
            sql = self._construct_sql_query(apply_limit=False)
            return self._execute_sql_query(sql)

    def _construct_sql_query(self, apply_limit: bool) -> str:
        clauses = []
        if self.projection is not None:
            clauses.append(f"SELECT {', '.join(self.projection)}")
        else:
            clauses.append("SELECT *")

        clauses.append(f"FROM ({self.sql}) AS subquery")

        if self.predicate is not None:
            clauses.append(f"WHERE {self.predicate}")

        if self.limit is not None and apply_limit is True:
            clauses.append(f"LIMIT {self.limit}")

        return "\n".join(clauses)

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

        db_type = urlparse(self.url).scheme
        db_type = db_type.strip().lower()

        # Check if the database type is supported
        if db_type in supported_dbs:
            return self._execute_sql_query_with_connectorx(sql)
        else:
            return self._execute_sql_query_with_sqlalchemy(sql)

    def _execute_sql_query_with_connectorx(self, sql: str) -> pa.Table:
        import connectorx as cx

        logger.info(f"Using connectorx to execute sql: {sql}")
        try:
            table = cx.read_sql(conn=self.url, query=sql, return_type="arrow")
            return table
        except Exception as e:
            raise RuntimeError(f"Failed to execute sql: {sql} with url: {self.url}, error: {e}") from e

    def _execute_sql_query_with_sqlalchemy(self, sql: str) -> pa.Table:
        from sqlalchemy import create_engine, text

        logger.info(f"Using sqlalchemy to execute sql: {sql}")
        try:
            with create_engine(self.url).connect() as connection:
                result = connection.execute(text(sql))
                rows = result.fetchall()
                pydict = {column_name: [row[i] for row in rows] for i, column_name in enumerate(result.keys())}

                # TODO: Use type codes from cursor description to create pyarrow schema
                return pa.Table.from_pydict(pydict)
        except Exception as e:
            raise RuntimeError(f"Failed to execute sql: {sql} with url: {self.url}, error: {e}") from e
