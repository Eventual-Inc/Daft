from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

import turbopuffer

from daft.datatype import DataType
from daft.dependencies import pc
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
        api_key: str,
        region: str | None = None,
        distance_metric: Literal["cosine_distance", "euclidean_squared"] | None = None,
        schema: dict[str, Any] | None = None,
        id_column: str | None = None,
        vector_column: str | None = None,
        client_kwargs: dict[str, Any] | None = None,
        write_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the Turbopuffer data sink.

        This data sink transforms each row of the dataframe into a turbopuffer document.
        This means that an `id` column is always required. Optionally, the `id_column` parameter can be used to specify the column name to used for the id column.
        Note that the column with the name specified by `id_column` will be renamed to "id" when written to turbopuffer.

        A `vector` column is required if the namespace has a vector index. Optionally, the `vector_column` parameter can be used to specify the column name to used for the vector index.
        Note that the column with the name specified by `vector_column` will be renamed to "vector" when written to turbopuffer.

        All other columns become attributes.

        For more details on parameters, please see the turbopuffer documentation: https://turbopuffer.com/docs/write

        Args:
            namespace: The namespace to write to.
            api_key: Turbopuffer API key.
            region: Turbopuffer region.
            distance_metric: Optional distance metric for vector similarity ("cosine_distance", "euclidean_squared").
            schema: Optional manual schema specification.
            id_column: Optional column name for the id column. The data sink will automatically rename the column to "id" for the id column.
            vector_column: Optional column name for the vector index column. The data sink will automatically rename the column to "vector" for the vector index.
            client_kwargs: Optional dictionary of arguments to pass to the Turbopuffer client constructor.
                Explicit arguments (api_key, region) will be merged into client_kwargs.
            write_kwargs: Optional dictionary of arguments to pass to the namespace.write() method.
                Explicit arguments (distance_metric, schema) will be merged into write_kwargs.
        """
        self._namespace_name = namespace
        self._api_key = api_key
        if not self._api_key:
            raise ValueError("Turbopuffer API key must be provided or set in TURBOPUFFER_API_KEY environment variable")

        self._region = region
        self._distance_metric = distance_metric
        self._schema = schema or {}
        self._id_column = id_column
        self._vector_column = vector_column

        # Merge explicit arguments into kwargs.
        self._client_kwargs = client_kwargs or {}
        if api_key is not None:
            if "api_key" in self._client_kwargs:
                raise ValueError("api_key is already set in client_kwargs")
            self._client_kwargs["api_key"] = api_key
        if region is not None:
            if "region" in self._client_kwargs:
                raise ValueError("region is already set in client_kwargs")
            self._client_kwargs["region"] = region

        self._write_kwargs = write_kwargs or {}
        if distance_metric is not None:
            if "distance_metric" in self._write_kwargs:
                raise ValueError("distance_metric is already set in write_kwargs")
            self._write_kwargs["distance_metric"] = distance_metric
        if schema is not None:
            if "schema" in self._write_kwargs:
                raise ValueError("schema is already set in write_kwargs")
            self._write_kwargs["schema"] = schema

        self._result_schema = Schema._from_field_name_and_types([("write_responses", DataType.python())])

    def schema(self) -> Schema:
        return self._result_schema

    def write(
        self, micropartitions: Iterator[MicroPartition]
    ) -> Iterator[WriteResult[turbopuffer.types.NamespaceWriteResponse]]:
        """Writes micropartitions to Turbopuffer namespace."""
        turbopuffer = self._import_turbopuffer()
        tpuf = turbopuffer.Turbopuffer(**self._client_kwargs)

        namespace = tpuf.namespace(self._namespace_name)

        for micropartition in micropartitions:
            arrow_table = micropartition.to_arrow()
            if self._id_column:
                if self._id_column not in arrow_table.column_names:
                    raise ValueError(f"ID column {self._id_column} not found in schema, cannot use as column for id")
                arrow_table = arrow_table.rename_columns({self._id_column: "id"})
            if self._vector_column:
                if self._vector_column not in arrow_table.column_names:
                    raise ValueError(
                        f"Vector column {self._vector_column} not found in schema, cannot use as column for vector index"
                    )
                arrow_table = arrow_table.rename_columns({self._vector_column: "vector"})

            arrow_table = arrow_table.filter(~pc.field("id").is_null())

            bytes_written = arrow_table.nbytes
            rows_written = arrow_table.num_rows

            write_response = namespace.write(
                upsert_rows=arrow_table.to_pylist(),
                **self._write_kwargs,
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
