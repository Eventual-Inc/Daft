import pathlib
from itertools import chain
from typing import Iterator, List, Literal, Optional, Union

import lance

from daft.context import get_context
from daft.daft import IOConfig
from daft.datatype import DataType
from daft.io import DataSink, WriteOutput
from daft.recordbatch import MicroPartition
from daft.schema import Schema


class LanceDataSink(DataSink[list[lance.FragmentMetadata]]):
    """WriteSink for writing data to a Lance dataset."""

    def _import_lance(self):
        try:
            import lance

            return lance
        except ImportError:
            raise ImportError("lance is not installed. Please install lance using `pip install daft[lance]`")

    def __init__(
        self,
        uri: Union[str, pathlib.Path],
        schema: Schema,
        mode: Literal["create", "append", "overwrite"],
        io_config: Optional[IOConfig] = None,
        **kwargs,
    ):
        from daft.dependencies import pa
        from daft.io.object_store_options import io_config_to_storage_options

        lance = self._import_lance()

        if not isinstance(uri, (str, pathlib.Path)):
            raise TypeError(f"Expected URI to be str or pathlib.Path, got {type(uri)}")
        self._table_uri = str(uri)
        self._mode = mode
        self._io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
        self._args = kwargs

        self._storage_options = io_config_to_storage_options(self._io_config, self._table_uri)

        self._pyarrow_schema = pa.schema((f.name, f.dtype.to_arrow_dtype()) for f in schema)

        try:
            table = lance.dataset(self._table_uri, storage_options=self._storage_options)

        except ValueError:
            table = None

        self._version = 0
        if table:
            table_schema = table.schema
            self._version = table.latest_version
            if self._pyarrow_schema != table_schema and not (self._mode == "overwrite"):
                raise ValueError(
                    "Schema of data does not match table schema\n"
                    f"Data schema:\n{self._pyarrow_schema}\nTable Schema:\n{table_schema}"
                )

        self._schema = Schema._from_field_name_and_types(
            [
                ("num_fragments", DataType.int64()),
                ("num_deleted_rows", DataType.int64()),
                ("num_small_files", DataType.int64()),
                ("version", DataType.int64()),
            ]
        )

    def schema(self) -> Schema:
        return self._schema

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteOutput[list[lance.FragmentMetadata]]]:
        """Writes fragments from the given micropartitions."""
        lance = self._import_lance()

        for micropartition in micropartitions:
            arrow_table = micropartition.to_arrow()
            bytes_written = arrow_table.nbytes
            rows_written = arrow_table.num_rows

            fragments = lance.fragment.write_fragments(
                arrow_table,
                dataset_uri=self._table_uri,
                mode=self._mode,
                storage_options=self._storage_options,
                **self._args,
            )
            yield WriteOutput(
                output=fragments,
                bytes_written=bytes_written,
                rows_written=rows_written,
            )

    def finalize(self, write_outputs: List[WriteOutput[list[lance.FragmentMetadata]]]) -> MicroPartition:
        """Commits the fragments to the Lance dataset. Returns a DataFrame with the stats of the dataset."""
        from daft.dependencies import pa

        lance = self._import_lance()

        fragments = list(chain.from_iterable(write_output.output for write_output in write_outputs))

        if self._mode == "create" or self._mode == "overwrite":
            operation = lance.LanceOperation.Overwrite(self._pyarrow_schema, fragments)
        elif self._mode == "append":
            operation = lance.LanceOperation.Append(fragments)

        dataset = lance.LanceDataset.commit(
            self._table_uri, operation, read_version=self._version, storage_options=self._storage_options
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
