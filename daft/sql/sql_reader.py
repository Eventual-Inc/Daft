from __future__ import annotations

import logging
from urllib.parse import urlparse

import pyarrow as pa

logger = logging.getLogger(__name__)


class SQLReader:
    def __init__(
        self,
        sql: str,
        url: str,
        left_bound: str | None = None,
        right_bound: str | None = None,
        limit: int | None = None,
        projection: list[str] | None = None,
        predicate: str | None = None,
    ) -> None:

        self.sql = sql
        self.url = url
        self.left_bound = left_bound
        self.right_bound = right_bound
        self.limit = limit
        self.projection = projection
        self.predicate = predicate

    def read(self) -> pa.Table:
        sql = self._construct_sql_query()
        return self._execute_sql_query(sql)

    def _construct_sql_query(self) -> str:
        base_query = f"SELECT * FROM ({self.sql}) AS subquery"
        if self.left_bound is not None and self.right_bound is not None:
            base_query = f"{base_query} WHERE {self.left_bound} AND {self.right_bound}"
        elif self.left_bound is not None:
            base_query = f"{base_query} WHERE {self.left_bound}"
        elif self.right_bound is not None:
            base_query = f"{base_query} WHERE {self.right_bound}"

        clauses = []
        if self.projection is not None:
            clauses.append(f"SELECT {', '.join(self.projection)}")
        else:
            clauses.append("SELECT *")

        clauses.append(f"FROM ({base_query}) AS subquery")

        if self.predicate is not None:
            clauses.append(f"WHERE {self.predicate}")

        if self.limit is not None:
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

                return pa.Table.from_pydict(pydict)
        except Exception as e:
            raise RuntimeError(f"Failed to execute sql: {sql} with url: {self.url}, error: {e}") from e
