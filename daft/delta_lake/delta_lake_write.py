from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Tuple

from daft.context import get_context
from daft.daft import IOConfig
from daft.datatype import DataType
from daft.dependencies import pa
from daft.io.common import _get_schema_from_dict
from daft.table.micropartition import MicroPartition
from daft.table.partitioning import PartitionedTable, partition_strings_to_path

if TYPE_CHECKING:
    from deltalake.writer import AddAction


def sanitize_table_for_deltalake(
    table: MicroPartition, large_dtypes: bool, partition_keys: Optional[List[str]] = None
) -> pa.Table:
    from deltalake.schema import _convert_pa_schema_to_delta

    from daft.io._deltalake import large_dtypes_kwargs

    arrow_table = table.to_arrow()

    # Remove partition keys from the table since they are already encoded as keys
    if partition_keys is not None:
        arrow_table = arrow_table.drop_columns(partition_keys)

    arrow_batch = _convert_pa_schema_to_delta(arrow_table.schema, **large_dtypes_kwargs(large_dtypes))
    return arrow_table.cast(arrow_batch)


def partitioned_table_to_deltalake_iter(
    partitioned: PartitionedTable, large_dtypes: bool
) -> Iterator[Tuple[pa.Table, str, Dict[str, Optional[str]]]]:
    """
    Iterates over partitions, yielding each partition as an Arrow table, along with their respective paths and partition values.
    """

    partition_values = partitioned.partition_values()

    if partition_values:
        partition_keys = partition_values.column_names()
        partition_strings = partitioned.partition_values_str()
        assert partition_strings is not None

        for part_table, part_strs in zip(partitioned.partitions(), partition_strings.to_pylist()):
            part_path = partition_strings_to_path("", part_strs)
            converted_arrow_table = sanitize_table_for_deltalake(part_table, large_dtypes, partition_keys)
            yield converted_arrow_table, part_path, part_strs
    else:
        converted_arrow_table = sanitize_table_for_deltalake(partitioned.table, large_dtypes)
        yield converted_arrow_table, "/", {}


def make_deltalake_add_action(
    path,
    metadata,
    size,
    partition_values,
):
    import json
    from datetime import datetime

    import deltalake
    from deltalake.writer import (
        AddAction,
        DeltaJSONEncoder,
        get_file_stats_from_metadata,
    )
    from packaging.version import parse

    # added to get_file_stats_from_metadata in deltalake v0.17.4: non-optional "num_indexed_cols" and "columns_to_collect_stats" arguments
    # https://github.com/delta-io/delta-rs/blob/353e08be0202c45334dcdceee65a8679f35de710/python/deltalake/writer.py#L725
    if parse(deltalake.__version__) < parse("0.17.4"):
        file_stats_args = {}
    else:
        file_stats_args = {"num_indexed_cols": -1, "columns_to_collect_stats": None}

    stats = get_file_stats_from_metadata(metadata, **file_stats_args)

    # remove leading slash
    path = path[1:] if path.startswith("/") else path
    return AddAction(
        path,
        size,
        partition_values,
        int(datetime.now().timestamp() * 1000),
        True,
        json.dumps(stats, cls=DeltaJSONEncoder),
    )


def make_deltalake_fs(path: str, io_config: Optional[IOConfig] = None):
    from deltalake.writer import DeltaStorageHandler
    from pyarrow.fs import PyFileSystem

    from daft.io.object_store_options import io_config_to_storage_options

    io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_options = io_config_to_storage_options(io_config, path)
    return PyFileSystem(DeltaStorageHandler(path, storage_options))


class DeltaLakeWriteVisitors:
    class FileVisitor:
        def __init__(
            self,
            parent: "DeltaLakeWriteVisitors",
            partition_values: Dict[str, Optional[str]],
        ):
            self.parent = parent
            self.partition_values = partition_values

        def __call__(self, written_file):
            from daft.utils import get_arrow_version

            # PyArrow added support for size in 9.0.0
            if get_arrow_version() >= (9, 0, 0):
                size = written_file.size
            elif self.parent.fs is not None:
                size = self.parent.fs.get_file_info([written_file.path])[0].size
            else:
                size = 0

            add_action = make_deltalake_add_action(
                written_file.path, written_file.metadata, size, self.partition_values
            )

            self.parent.add_actions.append(add_action)

    def __init__(self, fs: pa.fs.FileSystem):
        self.add_actions: List[AddAction] = []
        self.fs = fs

    def visitor(self, partition_values: Dict[str, Optional[str]]) -> "DeltaLakeWriteVisitors.FileVisitor":
        return self.FileVisitor(self, partition_values)

    def to_metadata(self) -> MicroPartition:
        col_name = "add_action"
        if len(self.add_actions) == 0:
            return MicroPartition.empty(_get_schema_from_dict({col_name: DataType.python()}))
        return MicroPartition.from_pydict({col_name: self.add_actions})
