from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

from daft.datatype import DataType
from daft.expressions import col, lit
from daft.functions import length, serialize
from daft.io import DataSink
from daft.io.sink import WriteResult
from daft.recordbatch import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import ModuleType

    from google.cloud.bigtable.table import Table

    from daft.dataframe import DataFrame


class BigtableDataSink(DataSink[dict[str, Any]]):
    """WriteSink for writing data to Google Cloud Bigtable."""

    @staticmethod
    def _import_bigtable() -> ModuleType:
        try:
            from google.cloud import bigtable

            return bigtable
        except ImportError:
            raise ImportError(
                "google-cloud-bigtable is not installed. Please install it using `pip install google-cloud-bigtable`"
            )

    @staticmethod
    def _import_bigtable_batcher() -> ModuleType:
        try:
            from google.cloud.bigtable import batcher

            return batcher
        except ImportError:
            raise ImportError(
                "google-cloud-bigtable is not installed. Please install it using `pip install google-cloud-bigtable`"
            )

    def __init__(
        self,
        project_id: str,
        instance_id: str,
        table_id: str,
        row_key_column: str,
        column_family_mappings: dict[str, str],
        client_kwargs: dict[str, Any] | None = None,
        write_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self._project_id = project_id
        self._instance_id = instance_id
        self._table_id = table_id
        self._column_family_mappings = column_family_mappings
        self._row_key_column = row_key_column
        self._client_kwargs = client_kwargs or {}
        self._write_kwargs = write_kwargs or {}

        self._result_schema = Schema._from_field_name_and_types([("write_responses", DataType.python())])

    @staticmethod
    def _is_bigtable_compatible_type(dtype: DataType) -> bool:
        """Check if a DataType is compatible with Bigtable."""
        return dtype.is_integer() or dtype.is_binary() or dtype.is_string()

    @staticmethod
    def _validate_bigtable_parameters(
        schema: Schema, row_key_column: str, column_family_mappings: dict[str, str]
    ) -> None:
        """Validate Bigtable parameters against the DataFrame schema."""
        column_names = schema.column_names()
        # Validate that the row key column exists in the schema.
        if row_key_column not in column_names:
            raise ValueError(f'Row key column "{row_key_column}" not found in schema')

        # Validate that column family mappings cover all columns except the row key column.
        data_columns = [col for col in column_names if col != row_key_column]
        missing_columns = [col for col in data_columns if col not in column_family_mappings]
        if missing_columns:
            raise ValueError(
                f"Column family mappings missing for columns: {missing_columns}. "
                f"All columns except the row key column ({row_key_column}) must have a column family mapping."
            )

    def _preprocess_dataframe(self, df: DataFrame, serialize_incompatible_types: bool = True) -> DataFrame:
        # Validate Bigtable parameters against the DataFrame schema.
        self._validate_bigtable_parameters(df.schema(), self._row_key_column, self._column_family_mappings)

        df_to_write = df

        # Check for incompatible types and handle them based on whether serialize_incompatible_types is True.
        incompatible_columns = []

        for field in df_to_write.schema():
            if self._is_bigtable_compatible_type(field.dtype):
                continue
            incompatible_columns.append(field.name)

        if incompatible_columns:
            if serialize_incompatible_types:
                warnings.warn(
                    f"Bigtable only supports integer, binary, and string types. Found incompatible columns: "
                    f"{', '.join([f'{column_name}' for column_name in incompatible_columns])}. "
                    "These will be automatically serialized to JSON. Set `serialize_incompatible_types=False` to disable automatic conversion."
                )

                for col_name in incompatible_columns:
                    df_to_write = df_to_write.with_column(col_name, serialize(df_to_write[col_name], format="json"))
            else:
                raise ValueError(
                    f"Bigtable only supports integer, binary, and string types. Found incompatible columns: "
                    f"{', '.join([f'{column_name}' for column_name in incompatible_columns])}. "
                    "Set `serialize_incompatible_types=True` to automatically convert these to JSON, or convert them manually before writing."
                )

        # Filter out rows with invalid row keys.
        df_to_write = df_to_write.filter(
            col(self._row_key_column).not_null() & (length(col(self._row_key_column)) > lit(0))
        )

        return df_to_write

    def name(self) -> str:
        return "Bigtable Data Sink"

    def schema(self) -> Schema:
        return self._result_schema

    def _write_with_error_handling(self, table: Table, mp: MicroPartition) -> WriteResult[dict[str, Any]]:
        try:
            bigtable_batcher = BigtableDataSink._import_bigtable_batcher()
            rows_written = 0

            with bigtable_batcher.MutationsBatcher(table=table, **self._write_kwargs) as batcher:
                rows = []
                data_list = mp.to_pylist()

                for row_data in data_list:
                    row_key = str(row_data[self._row_key_column])
                    direct_row = table.direct_row(row_key)
                    for column_name, value in row_data.items():
                        if column_name != self._row_key_column and value is not None:
                            direct_row.set_cell(
                                self._column_family_mappings[column_name],
                                column_name,
                                value,
                            )

                    rows.append(direct_row)

                if rows:
                    batcher.mutate_rows(rows)
                    rows_written = len(rows)

            result = {
                "status": "success",
                "rows_written": rows_written,
            }
            return WriteResult(
                result=result,
                bytes_written=mp.size_bytes() or 0,
                rows_written=rows_written,
            )
        except Exception as e:
            result = {
                "status": "failed",
                "error": str(e),
                "rows_not_written": len(mp),
            }
            return WriteResult(
                result=result,
                bytes_written=0,
                rows_written=0,
            )

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[dict[str, Any]]]:
        bigtable = BigtableDataSink._import_bigtable()

        # Handle emulator host configuration.
        client_kwargs = self._client_kwargs.copy()
        if "emulator_host" in client_kwargs:
            import os

            os.environ["BIGTABLE_EMULATOR_HOST"] = client_kwargs.pop("emulator_host")

        client = bigtable.Client(project=self._project_id, admin=True, **client_kwargs)
        instance = client.instance(self._instance_id)

        for micropartition in micropartitions:
            if len(micropartition) == 0:
                continue

            table = instance.table(self._table_id)
            yield self._write_with_error_handling(table, micropartition)

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
