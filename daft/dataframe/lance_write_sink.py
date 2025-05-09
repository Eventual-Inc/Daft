import pathlib
from typing import Iterator, List, Literal, Optional, Union

import lance

from daft.context import get_context
from daft.daft import IOConfig
from daft.dataframe.dataframe import DataFrame
from daft.io import DataSink
from daft.logical.schema import Schema
from daft.recordbatch import MicroPartition


class LanceWriteSink(DataSink[lance.FragmentMetadata, DataFrame]):
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
    ):
        from daft.dependencies import pa
        from daft.io.object_store_options import io_config_to_storage_options

        lance = self._import_lance()

        if not isinstance(uri, (str, pathlib.Path)):
            raise TypeError(f"Expected URI to be str or pathlib.Path, got {type(uri)}")
        self.table_uri = str(uri)
        self.mode = mode
        self.io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config

        self.storage_options = io_config_to_storage_options(self.io_config, self.table_uri)

        self.pyarrow_schema = pa.schema((f.name, f.dtype.to_arrow_dtype()) for f in schema)

        try:
            table = lance.dataset(self.table_uri, storage_options=self.storage_options)

        except ValueError:
            table = None

        self.version = 0
        if table:
            table_schema = table.schema
            self.version = table.latest_version
            if self.pyarrow_schema != table_schema and not (self.mode == "overwrite"):
                raise ValueError(
                    "Schema of data does not match table schema\n"
                    f"Data schema:\n{self.pyarrow_schema}\nTable Schema:\n{table_schema}"
                )

    def write(self, micropartitions: Iterator[MicroPartition], **kwargs) -> Iterator[lance.FragmentMetadata]:
        """Writes fragments from the given micropartitions."""
        lance = self._import_lance()

        for micropartition in micropartitions:
            arrow_table = micropartition.to_arrow()

            fragments = lance.fragment.write_fragments(
                arrow_table, dataset_uri=self.table_uri, mode=self.mode, storage_options=self.storage_options, **kwargs
            )

            yield from fragments

    def finish(self, results: List[lance.FragmentMetadata]) -> DataFrame:
        """Commits the fragments to the Lance dataset. Returns a DataFrame with the stats of the dataset."""
        from daft import from_pydict
        from daft.dependencies import pa

        lance = self._import_lance()

        fragments = results

        if self.mode == "create" or self.mode == "overwrite":
            operation = lance.LanceOperation.Overwrite(self.pyarrow_schema, fragments)
        elif self.mode == "append":
            operation = lance.LanceOperation.Append(fragments)

        dataset = lance.LanceDataset.commit(
            self.table_uri, operation, read_version=self.version, storage_options=self.storage_options
        )
        stats = dataset.stats.dataset_stats()

        tbl = from_pydict(
            {
                "num_fragments": pa.array([stats["num_fragments"]], type=pa.int64()),
                "num_deleted_rows": pa.array([stats["num_deleted_rows"]], type=pa.int64()),
                "num_small_files": pa.array([stats["num_small_files"]], type=pa.int64()),
                "version": pa.array([dataset.version], type=pa.int64()),
            }
        )
        return tbl
