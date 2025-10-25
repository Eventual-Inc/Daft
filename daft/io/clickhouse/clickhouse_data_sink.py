from __future__ import annotations

from typing import TYPE_CHECKING, Any

from clickhouse_connect import get_client
from clickhouse_connect.driver.summary import QuerySummary

from daft import Schema
from daft.datatype import DataType
from daft.io import DataSink
from daft.io.sink import WriteResult
from daft.recordbatch.micropartition import MicroPartition

if TYPE_CHECKING:
    from collections.abc import Iterator


class ClickHouseDataSink(DataSink[QuerySummary]):
    def __init__(
        self,
        table: str,
        *,
        host: str,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        client_kwargs: dict[str, Any] | None = None,
        write_kwargs: dict[str, Any] | None = None,
    ) -> None:
        connection_params: dict[str, Any] = {
            "host": host,
        }

        if port is not None:
            connection_params["port"] = port
        if user is not None:
            connection_params["user"] = user
        if password is not None:
            connection_params["password"] = password
        if database is not None:
            connection_params["database"] = database

        # Merge user-provided client_kwargs with explicit connection parameters
        self._client_kwargs = {**(client_kwargs or {}), **connection_params}
        self._table = table

        self._write_kwargs = write_kwargs or {}

        # Define the schema for the result of the write operation
        self._result_schema = Schema._from_field_name_and_types(
            [("total_written_rows", DataType.int64()), ("total_written_bytes", DataType.int64())]
        )

    def schema(self) -> Schema:
        return self._result_schema

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[QuerySummary]]:
        """Writes to ClickHouse from the given micropartitions."""
        # socket cannot be serialized, so we need to create a new client in write
        ck_client = get_client(**self._client_kwargs)
        try:
            for micropartition in micropartitions:
                df = micropartition.to_pandas()
                bytes_written: int = int(df.memory_usage().sum())
                rows_written = df.shape[0]

                query_summary = ck_client.insert_df(self._table, df, **self._write_kwargs)
                yield WriteResult(
                    result=query_summary,
                    bytes_written=bytes_written,
                    rows_written=rows_written,
                )
        finally:
            ck_client.close()

    def finalize(self, write_results: list[WriteResult[QuerySummary]]) -> MicroPartition:
        """Finish write to ClickHouse dataset. Returns a DataFrame with the stats of the dataset."""
        from daft.dependencies import pa

        total_written_rows = 0
        total_written_bytes = 0

        for write_result in write_results:
            total_written_rows += write_result.rows_written
            total_written_bytes += write_result.bytes_written

        tbl = MicroPartition.from_pydict(
            {
                "total_written_rows": pa.array([total_written_rows], pa.int64()),
                "total_written_bytes": pa.array([total_written_bytes], pa.int64()),
            }
        )
        return tbl
