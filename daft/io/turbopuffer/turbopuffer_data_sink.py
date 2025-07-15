from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Literal

import turbopuffer

from daft.datatype import DataType
from daft.io import DataSink
from daft.io.sink import WriteResult
from daft.recordbatch import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import ModuleType


class TurbopufferDataSink(DataSink[turbopuffer.types.NamespaceWriteResponse]):
    """WriteSink for writing data to a Turbopuffer namespace."""

    def _import_turbopuffer(self) -> ModuleType:
        try:
            import turbopuffer

            return turbopuffer
        except ImportError:
            raise ImportError(
                "turbopuffer is not installed. Please install turbopuffer using `pip install turbopuffer`"
            )

    def __init__(
        self,
        namespace: str,
        api_key: str | None = None,
        region: str = "aws-us-west-2",
        distance_metric: Literal["cosine_distance", "euclidean_squared"] | None = None,
        schema: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the Turbopuffer data sink.

        For more details on parameters, please see the turbopuffer documentation: https://turbopuffer.com/docs/write

        Args:
            namespace: The namespace to write to.
            api_key: Turbopuffer API key (defaults to TURBOPUFFER_API_KEY env var).
            region: Turbopuffer region (defaults to "aws-us-west-2").
            distance_metric: Distance metric for vector similarity ("cosine_distance", "euclidean_distance").
            schema: Optional manual schema specification.
            io_config: IO configuration
        """
        self._namespace_name = namespace
        self._api_key = api_key or os.getenv("TURBOPUFFER_API_KEY")
        if not self._api_key:
            raise ValueError("Turbopuffer API key must be provided or set in TURBOPUFFER_API_KEY environment variable")

        self._region = region
        self._distance_metric = distance_metric
        self._schema = schema or {}

        self._result_schema = Schema._from_field_name_and_types([("write_responses", DataType.python())])

    def schema(self) -> Schema:
        return self._result_schema

    def write(
        self, micropartitions: Iterator[MicroPartition]
    ) -> Iterator[WriteResult[turbopuffer.types.NamespaceWriteResponse]]:
        """Writes micropartitions to Turbopuffer namespace."""
        turbopuffer = self._import_turbopuffer()
        tpuf = turbopuffer.Turbopuffer(
            api_key=self._api_key,
            region=self._region,
        )

        namespace = tpuf.namespace(self._namespace_name)

        for micropartition in micropartitions:
            arrow_table = micropartition.to_arrow()
            bytes_written = arrow_table.nbytes
            rows_written = arrow_table.num_rows

            write_response = namespace.write(
                upsert_rows=arrow_table.to_pylist(),
                distance_metric=self._distance_metric,
                schema=self._schema,
            )

            yield WriteResult(
                result=write_response,
                bytes_written=bytes_written,
                rows_written=rows_written,
            )

    def finalize(self, write_results: list[WriteResult[turbopuffer.types.NamespaceWriteResponse]]) -> MicroPartition:
        """Finalizes the write process and returns summary statistics."""
        result_table = MicroPartition.from_pydict(
            {
                "write_responses": write_results,
            }
        )

        return result_table
