from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from qdrant_client import QdrantClient, models
from qdrant_client.http.exceptions import ResponseHandlingException, UnexpectedResponse

from daft.datatype import DataType
from daft.dependencies import pc
from daft.expressions import ExpressionsProjection
from daft.io import DataSink
from daft.io.sink import WriteResult
from daft.recordbatch import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import ModuleType

    from daft.dependencies import pa
    from daft.expressions import Expression


_TRANSIENT_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})


class QdrantDataSink(DataSink[dict[str, Any]]):
    _collection: str | ExpressionsProjection

    @staticmethod
    def _import_qdrant() -> ModuleType:
        try:
            import qdrant_client

            return qdrant_client
        except ImportError:
            raise ImportError(
                "qdrant-client is not installed. Please install qdrant-client using `pip install qdrant-client`"
            )

    @staticmethod
    def _check_collection_name(collection_name: str) -> None:
        if not isinstance(collection_name, str):
            raise ValueError(
                f"Collection name must be a string, got {collection_name} with type {type(collection_name)}"
            )

    def __init__(
        self,
        collection_name: str | Expression,
        url: str | None = None,
        api_key: str | None = None,
        id_column: str | None = None,
        vector_column: str | None = None,
        client_kwargs: dict[str, Any] | None = None,
        upsert_kwargs: dict[str, Any] | None = None,
    ) -> None:
        if isinstance(collection_name, str):
            QdrantDataSink._check_collection_name(collection_name)
            self._collection = collection_name
        else:
            self._collection = ExpressionsProjection([collection_name])

        self._api_key = api_key or os.getenv("QDRANT_API_KEY")

        self._url = url
        self._id_column = id_column
        self._vector_column = vector_column

        self._client_kwargs = client_kwargs or {}
        if url is not None:
            if "url" in self._client_kwargs:
                raise ValueError("url is already set in client_kwargs")
            self._client_kwargs["url"] = url
        if api_key is not None:
            if "api_key" in self._client_kwargs:
                raise ValueError("api_key is already set in client_kwargs")
            self._client_kwargs["api_key"] = api_key

        self._upsert_kwargs = upsert_kwargs or {}

        self._result_schema = Schema._from_field_name_and_types([("write_responses", DataType.python())])

    def name(self) -> str:
        return "Qdrant Write"

    def schema(self) -> Schema:
        return self._result_schema

    def _prepare_arrow_table(self, arrow_table: pa.Table) -> pa.Table:
        if self._id_column:
            if self._id_column not in arrow_table.column_names:
                raise ValueError(f"ID column {self._id_column} not found in schema, cannot use as column for id")
            arrow_table = arrow_table.rename_columns({self._id_column: "id"})

        if self._vector_column:
            if self._vector_column not in arrow_table.column_names:
                raise ValueError(
                    f"Vector column {self._vector_column} not found in schema, cannot use as column for vector"
                )
            arrow_table = arrow_table.rename_columns({self._vector_column: "vector"})

        id_column = arrow_table.column("id")
        mask = pc.invert(pc.is_null(id_column))
        return arrow_table.filter(mask)

    @staticmethod
    def _build_points(arrow_table: pa.Table) -> list[models.PointStruct]:
        points = []
        for row in arrow_table.to_pylist():
            point_id = row.pop("id")
            vector = row.pop("vector")
            points.append(models.PointStruct(id=point_id, vector=vector, payload=row))
        return points

    def _write_with_error_handling(
        self, client: QdrantClient, collection_name: str, arrow_table: pa.Table
    ) -> WriteResult[dict[str, Any]]:
        points = QdrantDataSink._build_points(arrow_table)
        try:
            response = client.upsert(
                collection_name=collection_name,
                points=points,
                **self._upsert_kwargs,
            )
            result = {
                "status": "success",
                "response": response,
            }
            return WriteResult(
                result=result,
                bytes_written=arrow_table.nbytes,
                rows_written=arrow_table.num_rows,
            )
        except (ResponseHandlingException, UnexpectedResponse) as e:
            # Network/timeout errors and transient HTTP status codes are handled gracefully so that
            # writes can continue. Other UnexpectedResponse codes (e.g. 4xx) propagate.
            if isinstance(e, UnexpectedResponse) and e.status_code not in _TRANSIENT_STATUS_CODES:
                raise
            result = {
                "status": "failed",
                "error": str(e),
                "rows_not_written": arrow_table.num_rows,
            }
            return WriteResult(
                result=result,
                bytes_written=0,
                rows_written=0,
            )

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[dict[str, Any]]]:
        qdrant_client = QdrantDataSink._import_qdrant()
        client = qdrant_client.QdrantClient(**self._client_kwargs)

        for micropartition in micropartitions:
            if isinstance(self._collection, str):
                arrow_table = self._prepare_arrow_table(micropartition.to_arrow())
                if arrow_table.num_rows == 0:
                    continue
                yield self._write_with_error_handling(client, self._collection, arrow_table)
            else:
                (partitioned_data, partition_keys) = micropartition.partition_by_value(self._collection)
                for data, collection_name in zip(partitioned_data, partition_keys.get_column(0)):
                    arrow_table = self._prepare_arrow_table(data.to_arrow())
                    if arrow_table.num_rows == 0:
                        continue
                    QdrantDataSink._check_collection_name(collection_name)
                    yield self._write_with_error_handling(client, collection_name, arrow_table)

    def finalize(self, write_results: list[WriteResult[dict[str, Any]]]) -> MicroPartition:
        if len(write_results) == 0:
            return MicroPartition.empty(self._result_schema)
        else:
            result_table = MicroPartition.from_pydict(
                {
                    "write_responses": write_results,
                }
            )

            return result_table
