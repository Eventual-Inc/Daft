from __future__ import annotations

import os
import warnings
from typing import TYPE_CHECKING, Any, Literal

import turbopuffer

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


class TurbopufferDataSink(DataSink[dict[str, Any]]):
    """WriteSink for writing data to a Turbopuffer namespace."""

    _namespace: str | ExpressionsProjection

    @staticmethod
    def _import_turbopuffer() -> ModuleType:
        try:
            import turbopuffer

            return turbopuffer
        except ImportError:
            raise ImportError(
                "turbopuffer is not installed. Please install turbopuffer using `pip install turbopuffer`"
            )

    @staticmethod
    def _check_namespace_name(namespace_name: str) -> None:
        if not isinstance(namespace_name, str):
            raise ValueError(f"Namespace name must be a string, got {namespace_name} with type {type(namespace_name)}")
        if not (1 <= len(namespace_name) <= 128):
            raise ValueError(
                f"Namespace name must be between 1 and 128 characters, got length {len(namespace_name)}. For more details, see: https://turbopuffer.com/docs/write"
            )
        if not all(c.isalnum() or c in "-_." for c in namespace_name):
            raise ValueError(
                f"Namespace name can only contain alphanumeric characters, hyphens, underscores and periods. Got: {namespace_name}. For more details, see: https://turbopuffer.com/docs/write"
            )
        return

    def __init__(
        self,
        namespace: str | Expression,
        api_key: str | None = None,
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

        The namespace parameter can be either a string (for a single namespace) or an expression (for multiple namespaces).
        When using an expression, the data will be partitioned by the computed namespace values and written to each namespace separately.

        For more details on parameters, please see the turbopuffer documentation: https://turbopuffer.com/docs/write

        Args:
            namespace: The namespace to write to. Can be a string for a single namespace or an expression for multiple namespaces.
            api_key: Turbopuffer API key.
            region: Turbopuffer region.
            distance_metric: Distance metric for vector similarity ("cosine_distance", "euclidean_squared").
            schema: Optional manual schema specification.
            id_column: Optional column name for the id column. The data sink will automatically rename the column to "id" for the id column.
            vector_column: Optional column name for the vector index column. The data sink will automatically rename the column to "vector" for the vector index.
            client_kwargs: Optional dictionary of arguments to pass to the Turbopuffer client constructor.
                Explicit arguments (api_key, region) will be merged into client_kwargs.
            write_kwargs: Optional dictionary of arguments to pass to the namespace.write() method.
                Explicit arguments (distance_metric, schema) will be merged into write_kwargs.
        """
        if isinstance(namespace, str):
            TurbopufferDataSink._check_namespace_name(namespace)
            self._namespace = namespace
        else:
            self._namespace = ExpressionsProjection([namespace])

        self._api_key = api_key or os.getenv("TURBOPUFFER_API_KEY")
        if not self._api_key:
            warnings.warn("Turbopuffer API key is not set and TURBOPUFFER_API_KEY environment variable is not found")

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

    def name(self) -> str:
        return "Turbopuffer Write"

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
                    f"Vector column {self._vector_column} not found in schema, cannot use as column for vector index"
                )
            arrow_table = arrow_table.rename_columns({self._vector_column: "vector"})

        # Use compute function approach for pyarrow 8.0.0 compatibility
        id_column = arrow_table.column("id")
        mask = pc.invert(pc.is_null(id_column))
        return arrow_table.filter(mask)

    def _write_with_error_handling(
        self, namespace: turbopuffer.Namespace, arrow_table: pa.Table
    ) -> WriteResult[dict[str, Any]]:
        try:
            write_response = namespace.write(
                upsert_rows=arrow_table.to_pylist(),
                **self._write_kwargs,
            )
            result = {
                "status": "success",
                "response": write_response,
            }
            return WriteResult(
                result=result,
                bytes_written=arrow_table.nbytes,
                rows_written=arrow_table.num_rows,
            )
        except (
            turbopuffer.APIConnectionError,
            turbopuffer.InternalServerError,
            turbopuffer.RateLimitError,
            turbopuffer.ConflictError,
            turbopuffer.APITimeoutError,
        ) as e:
            # Turbopuffer client already retries transient errors. If, after the client library
            # has already retried but we still get transient errors, then we gracefully handle it
            # and allow writes to continue.
            #
            # For non-transient errors, we return an error result and allow the script to fail fast.
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
        """Writes micropartitions to Turbopuffer namespace(s)."""
        turbopuffer = TurbopufferDataSink._import_turbopuffer()
        tpuf = turbopuffer.Turbopuffer(**self._client_kwargs)

        for micropartition in micropartitions:
            if isinstance(self._namespace, str):
                # Namespace is a string. Write all data to this namespace.
                arrow_table = self._prepare_arrow_table(micropartition.to_arrow())
                if arrow_table.num_rows == 0:
                    continue

                namespace = tpuf.namespace(self._namespace)
                yield self._write_with_error_handling(namespace, arrow_table)
            else:
                # Namespace is an expression. Partition the data by namespace then write to each namespace.
                (partitioned_data, partition_keys) = micropartition.partition_by_value(self._namespace)
                for data, namespace_name in zip(partitioned_data, partition_keys.get_column(0)):
                    arrow_table = self._prepare_arrow_table(data.to_arrow())
                    if arrow_table.num_rows == 0:
                        continue

                    TurbopufferDataSink._check_namespace_name(namespace_name)
                    namespace = tpuf.namespace(namespace_name)
                    yield self._write_with_error_handling(namespace, arrow_table)

    def finalize(self, write_results: list[WriteResult[dict[str, Any]]]) -> MicroPartition:
        """Finalizes the write process and returns summary statistics."""
        if len(write_results) == 0:
            return MicroPartition.empty(self._result_schema)
        else:
            result_table = MicroPartition.from_pydict(
                {
                    "write_responses": write_results,
                }
            )

            return result_table
