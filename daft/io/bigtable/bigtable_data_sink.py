from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.datatype import DataType
from daft.dependencies import pc
from daft.io import DataSink
from daft.io.sink import WriteResult
from daft.recordbatch import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import ModuleType

    from google.cloud.bigtable.table import Table

    from daft.dependencies import pa


class BigTableDataSink(DataSink[dict[str, Any]]):
    """WriteSink for writing data to Google Cloud BigTable."""

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
        """Initialize the BigTable data sink.

        This data sink transforms each row of the dataframe into BigTable rows.
        A row key is always required. The `row_key_column` parameter can be used to specify the column name to use for the row key.

        All other columns become cells in the specified column family.

        Args:
            project_id: The Google Cloud project ID.
            instance_id: The BigTable instance ID.
            table_id: The table to write to.
            row_key_column: Column name for the row key.
            column_family_mappings: Mapping of column names to column families.
            client_kwargs: Optional dictionary of arguments to pass to the BigTable Client constructor.
            write_kwargs: Optional dictionary of arguments to pass to the BigTable MutationsBatcher.
        """
        self._project_id = project_id
        self._instance_id = instance_id
        self._table_id = table_id
        self._column_family_mappings = column_family_mappings
        self._row_key_column = row_key_column
        self._client_kwargs = client_kwargs or {}
        self._write_kwargs = write_kwargs or {}

        self._result_schema = Schema._from_field_name_and_types([("write_responses", DataType.python())])

    def name(self) -> str:
        return "BigTable Data Sink"

    def schema(self) -> Schema:
        return self._result_schema

    def _prepare_arrow_table(self, arrow_table: pa.Table) -> pa.Table:
        if self._row_key_column not in arrow_table.column_names:
            raise ValueError(f"Row key column {self._row_key_column} not found in schema")

        # Validate that column family mappings cover all columns except the row key column.
        data_columns = [col for col in arrow_table.column_names if col != self._row_key_column]
        missing_columns = [col for col in data_columns if col not in self._column_family_mappings]
        if missing_columns:
            raise ValueError(
                f"Column family mappings missing for columns: {missing_columns}. "
                f"All columns except the row key column ({self._row_key_column}) must have a column family mapping."
            )

        # Use compute function approach for pyarrow 8.0.0 compatibility.
        id_column = arrow_table.column(self._row_key_column)
        mask = pc.invert(pc.is_null(id_column))
        return arrow_table.filter(mask)

    def _write_with_error_handling(self, table: Table, arrow_table: pa.Table) -> WriteResult[dict[str, Any]]:
        try:
            bigtable_batcher = BigTableDataSink._import_bigtable_batcher()
            rows_written = 0

            with bigtable_batcher.MutationsBatcher(table=table, **self._write_kwargs) as batcher:
                rows = []
                data_list = arrow_table.to_pylist()

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
                bytes_written=arrow_table.nbytes,
                rows_written=rows_written,
            )
        except Exception as e:
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
        bigtable = BigTableDataSink._import_bigtable()
        client = bigtable.Client(project=self._project_id, admin=True, **self._client_kwargs)
        instance = client.instance(self._instance_id)

        for micropartition in micropartitions:
            arrow_table = self._prepare_arrow_table(micropartition.to_arrow())

            if arrow_table.num_rows == 0:
                continue

            table = instance.table(self._table_id)
            yield self._write_with_error_handling(table, arrow_table)

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
