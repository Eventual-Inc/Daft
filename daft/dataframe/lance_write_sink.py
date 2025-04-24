import pathlib
from typing import TYPE_CHECKING, Literal, Optional, Union

from daft.context import get_context
from daft.daft import IOConfig
from daft.dataframe.dataframe import DataFrame, WriteSink

if TYPE_CHECKING:
    import lance


class LanceWriteSink(WriteSink[list["lance.fragment.FragmentMetadata"]]):
    """WriteSink for writing data to a Lance dataset."""

    def __init__(
        self,
        uri: Union[str, pathlib.Path],
        mode: Literal["create", "append", "overwrite"],
        io_config: Optional[IOConfig] = None,
    ):
        from daft.io.object_store_options import io_config_to_storage_options

        if not isinstance(uri, (str, pathlib.Path)):
            raise TypeError(f"Expected URI to be str or pathlib.Path, got {type(uri)}")
        self.table_uri = str(uri)
        self.mode = mode
        self.io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config

        self.storage_options = io_config_to_storage_options(self.io_config, self.table_uri)
        self.pyarrow_schema = None
        self.version = 0

    def _import_lance(self):
        try:
            import lance

            return lance
        except ImportError:
            raise ImportError("lance is not installed. Please install lance using `pip install daft[lance]`")

    def write(self, dataframe: DataFrame, **kwargs) -> list["lance.fragment.FragmentMetadata"]:
        """Takes in a DataFrame and returns the fragments that make up the new Lance dataset."""
        from daft.dependencies import pa

        lance = self._import_lance()

        self.pyarrow_schema = pa.schema((f.name, f.dtype.to_arrow_dtype()) for f in dataframe.schema())

        try:
            table = lance.dataset(self.table_uri, storage_options=self.storage_options)
        except ValueError:
            table = None

        if table:
            table_schema = table.schema
            self.version = table.latest_version
            if self.pyarrow_schema != table_schema and not (self.mode == "overwrite"):
                raise ValueError(
                    "Schema of data does not match table schema\n"
                    f"Data schema:\n{self.pyarrow_schema}\nTable Schema:\n{table_schema}"
                )

        builder = dataframe._builder.write_lance(
            self.table_uri,
            self.mode,
            io_config=self.io_config,
            kwargs=kwargs,
        )
        write_df = DataFrame(builder)
        write_df.collect()

        write_result = write_df.to_pydict()
        assert "fragments" in write_result
        fragments = write_result["fragments"]
        return fragments

    def finish(self, fragments: list["lance.fragment.FragmentMetadata"]) -> "DataFrame":
        """Commits the fragments to the Lance dataset."""
        from daft import from_pydict
        from daft.dependencies import pa

        lance = self._import_lance()

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
