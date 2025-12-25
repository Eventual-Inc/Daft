from __future__ import annotations

import pathlib
from itertools import chain
from typing import TYPE_CHECKING, Any, Literal

import lance

from daft.context import get_context
from daft.datatype import DataType
from daft.dependencies import pa
from daft.io import DataSink
from daft.io.sink import WriteResult
from daft.recordbatch import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import ModuleType

    from daft.daft import IOConfig


def pyarrow_schema_castable(src: pa.Schema, dst: pa.Schema) -> bool:
    if len(src) != len(dst):
        return False
    for src_field, dst_field in zip(src, dst):
        empty_array = pa.array([], type=src_field.type)
        try:
            empty_array.cast(dst_field.type)
        except Exception:
            return False
    return True


def table_schema_subset_castable(data_schema: pa.Schema, table_schema: pa.Schema) -> bool:
    """Checks whether data_schema is a superset of table_schema and every table field is castable from data.

    For each field in the existing table schema, ensure there is a field with the same name in the data schema,
    and that Arrow can cast from the data field type to the table field type.
    """
    data_fields_by_name = {f.name: f for f in data_schema}
    for table_field in table_schema:
        src_field = data_fields_by_name.get(table_field.name)
        if src_field is None:
            return False
        empty_array = pa.array([], type=src_field.type)
        try:
            empty_array.cast(table_field.type)
        except Exception:
            return False
    return True


class LanceDataSink(DataSink[list[lance.FragmentMetadata]]):
    """WriteSink for writing data to a Lance dataset."""

    def _import_lance(self) -> ModuleType:
        try:
            import lance

            return lance
        except ImportError:
            raise ImportError("lance is not installed. Please install lance using `pip install daft[lance]`")

    def __init__(
        self,
        uri: str | pathlib.Path,
        schema: Schema | pa.Schema,
        mode: Literal["create", "append", "overwrite", "merge"],
        io_config: IOConfig | None = None,
        **kwargs: Any,
    ) -> None:
        from daft.io.object_store_options import io_config_to_storage_options

        lance = self._import_lance()
        if not isinstance(uri, (str, pathlib.Path)):
            raise TypeError(f"Expected URI to be str or pathlib.Path, got {type(uri)}")
        self._table_uri = str(uri)
        self._mode = mode
        self._io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
        self._kwargs = kwargs
        # Strip merge-specific kwargs that are not applicable to non-merge write paths
        self._kwargs.pop("left_on", None)
        self._kwargs.pop("right_on", None)
        self._storage_options = io_config_to_storage_options(self._io_config, self._table_uri)

        if isinstance(schema, Schema):
            self._pyarrow_schema = schema.to_pyarrow_schema()
        elif isinstance(schema, pa.Schema):
            self._pyarrow_schema = schema
        else:
            raise TypeError(f"Expected schema to be Schema or pa.Schema, got {type(schema)}")

        # Filter out internal metadata fields that should not be part of the validation
        # These fields are used internally by Lance but should not affect schema validation
        meta_exclusions = {"fragment_id", "_rowaddr", "_rowid"}
        # Create a filtered schema without internal metadata fields for validation
        filtered_fields = [field for field in self._pyarrow_schema if field.name not in meta_exclusions]
        self._filtered_schema = pa.schema(filtered_fields)

        self._version: int = 0
        self._table_schema: pa.Schema | None = None

        table = self._load_existing_dataset(lance)
        self._validate_dataset(table)
        self._schema = Schema._from_field_name_and_types(
            [
                ("num_fragments", DataType.int64()),
                ("num_deleted_rows", DataType.int64()),
                ("num_small_files", DataType.int64()),
                ("version", DataType.int64()),
            ]
        )

    def _load_existing_dataset(self, lance_module: ModuleType) -> Any | None:
        try:
            return lance_module.dataset(self._table_uri, storage_options=self._storage_options)
        except (ValueError, FileNotFoundError, OSError) as e:
            # Check if this is specifically a "dataset not found" error
            if "not found" in str(e).lower() or "does not exist" in str(e).lower():
                if self._mode == "append":
                    raise ValueError("Cannot append to non-existent Lance dataset.")
                return None
            else:
                # Re-raise other errors (permissions, network, etc.)
                raise

    def _validate_dataset(self, table: Any) -> None:
        """Validate dataset against the current mode."""
        if table is None:
            if self._mode == "append" or self._mode == "merge":
                raise ValueError("Cannot append or merge to non-existent Lance dataset.")
            if self._mode == "create" and self._storage_options is None:
                p = pathlib.Path(self._table_uri)
                if p.is_file():
                    raise FileExistsError("Target path points to a file, cannot create a dataset here.")
        else:
            self._table_schema = table.schema
            self._version = table.latest_version

            if self._mode == "create":
                raise ValueError("Cannot create a Lance dataset at a location where one already exists.")

            if self._mode == "append" and not pyarrow_schema_castable(self._pyarrow_schema, self._table_schema):
                raise ValueError(
                    "Schema of data does not match table schema\n"
                    f"Data schema:\n{self._pyarrow_schema}\nTable Schema:\n{self._table_schema}"
                )

    def name(self) -> str:
        """Optional custom sink name."""
        return "Lance Write"

    def schema(self) -> Schema:
        return self._schema

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[list[lance.FragmentMetadata]]]:
        """Writes fragments from the given micropartitions."""
        lance = self._import_lance()

        for micropartition in micropartitions:
            # Build an Arrow table that conforms to either:
            # - the existing dataset schema (if table already exists), or
            # - the user-provided schema (if specified and different from incoming data), or
            # - the incoming data schema (no-op) while attaching metadata/order from _pyarrow_schema.
            input_table = micropartition.to_arrow()

            if self._mode == "merge":
                target_schema = self._filtered_schema
            else:
                target_schema = self._table_schema

            # Handle case where target_schema is None (e.g., creating a new dataset)
            if target_schema is None:
                # Use the user-provided schema or the input schema
                target_schema = self._pyarrow_schema

            if self._table_schema is not None:
                # Dataset exists: always cast to the table schema to ensure compatibility on append
                arrow_table = input_table.cast(self._table_schema)
            elif not pa.Schema.equals(target_schema, input_table.schema):
                # New dataset or overwrite with a user-provided schema: cast to enforce order/types/nullability
                arrow_table = input_table.cast(target_schema)
            else:
                # Schemas are identical: rebuild table with the desired schema instance to preserve metadata
                arrow_table = pa.Table.from_batches(input_table.to_batches(), target_schema)

            bytes_written = arrow_table.nbytes
            rows_written = arrow_table.num_rows

            fragments = lance.fragment.write_fragments(
                arrow_table,
                dataset_uri=self._table_uri,
                mode=self._mode,
                storage_options=self._storage_options,
                **self._kwargs,
            )
            yield WriteResult(
                result=fragments,
                bytes_written=bytes_written,
                rows_written=rows_written,
            )

    def finalize(self, write_results: list[WriteResult[list[lance.FragmentMetadata]]]) -> MicroPartition:
        """Commits the fragments to the Lance dataset. Returns a DataFrame with the stats of the dataset."""
        lance = self._import_lance()

        fragments = list(chain.from_iterable(write_result.result for write_result in write_results))

        if self._mode == "create" or self._mode == "overwrite":
            operation = lance.LanceOperation.Overwrite(self._pyarrow_schema, fragments)
        elif self._mode == "append":
            operation = lance.LanceOperation.Append(fragments)

        dataset = lance.LanceDataset.commit(
            self._table_uri,
            operation,
            read_version=self._version,
            storage_options=self._storage_options,
        )
        stats = dataset.stats.dataset_stats()

        tbl = MicroPartition.from_pydict(
            {
                "num_fragments": pa.array([stats["num_fragments"]], type=pa.int64()),
                "num_deleted_rows": pa.array([stats["num_deleted_rows"]], type=pa.int64()),
                "num_small_files": pa.array([stats["num_small_files"]], type=pa.int64()),
                "version": pa.array([dataset.version], type=pa.int64()),
            }
        )
        return tbl
