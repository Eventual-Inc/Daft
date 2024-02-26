from __future__ import annotations

import logging

import pyarrow as pa

logger = logging.getLogger(__name__)


class SQLReader:
    def __init__(
        self,
        sql: str,
        url: str,
        limit: int | None = None,
        offset: int | None = None,
        limit_before_offset: bool | None = None,
        projection: list[str] | None = None,
        predicate: str | None = None,
    ) -> None:
        if limit is not None and offset is not None and limit_before_offset is None:
            raise ValueError("limit_before_offset must be specified when limit and offset are both specified")

        self.sql = sql
        self.url = url
        self.limit = limit
        self.offset = offset
        self.limit_before_offset = limit_before_offset
        self.projection = projection
        self.predicate = predicate

    def get_num_rows(self) -> int:
        sql = f"SELECT COUNT(*) FROM ({self.sql}) AS subquery"
        pa_table = self._execute_sql_query(sql)
        return pa_table.column(0)[0].as_py()

    def read(self) -> pa.Table:
        sql = self._construct_sql_query()
        return self._execute_sql_query(sql)

    def _construct_sql_query(self) -> str:
        if self.projection is not None:
            columns = ", ".join(self.projection)
        else:
            columns = "*"

        sql = f"SELECT {columns} FROM ({self.sql}) AS subquery"

        if self.predicate is not None:
            sql += f" WHERE {self.predicate}"

        if self.limit is not None and self.offset is not None:
            if self.limit_before_offset:
                sql += f" LIMIT {self.limit} OFFSET {self.offset}"
            else:
                sql += f" OFFSET {self.offset} LIMIT {self.limit}"
        elif self.limit is not None:
            sql += f" LIMIT {self.limit}"
        elif self.offset is not None:
            sql += f" OFFSET {self.offset}"

        return sql

    def _execute_sql_query(self, sql: str) -> pa.Table:
        # Supported DBs extracted from here https://github.com/sfu-db/connector-x/tree/7b3147436b7e20b96691348143d605e2249d6119?tab=readme-ov-file#sources
        supported_dbs = {"postgres", "mysql", "mssql", "oracle", "bigquery", "sqlite", "clickhouse", "redshift"}

        # Extract the database type from the URL
        db_type = self.url.split("://")[0]

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
        import pandas as pd
        from sqlalchemy import create_engine, text

        logger.info(f"Using sqlalchemy to execute sql: {sql}")
        try:
            with create_engine(self.url).connect() as connection:
                result = connection.execute(text(sql))
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                table = pa.Table.from_pandas(df)
                return table
        except Exception as e:
            raise RuntimeError(f"Failed to execute sql: {sql} with url: {self.url}, error: {e}") from e
