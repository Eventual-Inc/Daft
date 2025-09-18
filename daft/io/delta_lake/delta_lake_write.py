from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from daft.context import get_context
from daft.datatype import DataType
from daft.io.common import _get_schema_from_dict
from daft.recordbatch.micropartition import MicroPartition
from daft.recordbatch.partitioning import PartitionedTable, partition_strings_to_path

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from deltalake.writer import AddAction

    from daft.daft import IOConfig
    from daft.dependencies import pa, pads, pafs

try:
    import deltalake
    from packaging.version import parse

    if parse(deltalake.__version__) < parse("1.0.0"):
        from deltalake.writer import AddAction
    else:
        from deltalake.transaction import AddAction
except ImportError:
    pass


def sanitize_table_for_deltalake(
    table: MicroPartition, large_dtypes: bool, partition_keys: list[str] | None = None
) -> pa.Table:
    arrow_table = table.to_arrow()

    # Remove partition keys from the table since they are already encoded as keys
    if partition_keys is not None:
        arrow_table = arrow_table.drop_columns(partition_keys)

    arrow_batch = convert_pa_schema_to_delta(arrow_table.schema, large_dtypes)
    return arrow_table.cast(arrow_batch)


def partitioned_table_to_deltalake_iter(
    partitioned: PartitionedTable, large_dtypes: bool
) -> Iterator[tuple[pa.Table, str, dict[str, str | None]]]:
    """Iterates over partitions, yielding each partition as an Arrow table, along with their respective paths and partition values."""
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


def get_file_stats_from_metadata(
    metadata: Any,
) -> dict[str, int | dict[str, Any]]:
    from math import inf

    stats = {
        "numRecords": metadata.num_rows,
        "minValues": {},
        "maxValues": {},
        "nullCount": {},
    }

    def iter_groups(metadata: Any) -> Iterator[Any]:
        for i in range(metadata.num_row_groups):
            if metadata.row_group(i).num_rows > 0:
                yield metadata.row_group(i)

    for column_idx in range(metadata.num_columns):
        name = metadata.row_group(0).column(column_idx).path_in_schema

        # If stats missing, then we can't know aggregate stats
        if all(group.column(column_idx).is_stats_set for group in iter_groups(metadata)):
            stats["nullCount"][name] = sum(
                group.column(column_idx).statistics.null_count for group in iter_groups(metadata)
            )

            # Min / max may not exist for some column types, or if all values are null
            if any(group.column(column_idx).statistics.has_min_max for group in iter_groups(metadata)):
                # Min and Max are recorded in physical type, not logical type
                # https://stackoverflow.com/questions/66753485/decoding-parquet-min-max-statistics-for-decimal-type
                # TODO: Add logic to decode physical type for DATE, DECIMAL

                minimums = (group.column(column_idx).statistics.min for group in iter_groups(metadata))
                # If some row groups have all null values, their min and max will be null too.
                min_value = min(minimum for minimum in minimums if minimum is not None)
                # Infinity cannot be serialized to JSON, so we skip it. Saying
                # min/max is infinity is equivalent to saying it is null, anyways.
                if min_value != -inf:
                    stats["minValues"][name] = min_value
                maximums = (group.column(column_idx).statistics.max for group in iter_groups(metadata))
                max_value = max(maximum for maximum in maximums if maximum is not None)
                if max_value != inf:
                    stats["maxValues"][name] = max_value
    return stats


class DeltaJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        from datetime import date, datetime
        from decimal import Decimal

        if isinstance(obj, bytes):
            return obj.decode("unicode_escape", "backslashreplace")
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return str(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def make_deltalake_add_action(
    path: str,
    metadata: Any,
    size: int,
    partition_values: Mapping[str, str | None],
) -> AddAction:
    import json
    from datetime import datetime

    stats = get_file_stats_from_metadata(metadata)

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


def make_deltalake_fs(path: str, io_config: IOConfig | None = None) -> pafs.PyFileSystem:
    from deltalake.fs import DeltaStorageHandler
    from pyarrow.fs import PyFileSystem

    from daft.io.object_store_options import io_config_to_storage_options

    io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_options = io_config_to_storage_options(io_config, path)
    return PyFileSystem(DeltaStorageHandler(path, storage_options))


class DeltaLakeWriteVisitors:
    class FileVisitor:
        def __init__(
            self,
            parent: DeltaLakeWriteVisitors,
            partition_values: dict[str, str | None],
        ):
            self.parent = parent
            self.partition_values = partition_values

        def __call__(self, written_file: pads.WrittenFile) -> None:
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

    def __init__(self, fs: pafs.FileSystem):
        self.add_actions: list[AddAction] = []
        self.fs = fs

    def visitor(self, partition_values: dict[str, str | None]) -> DeltaLakeWriteVisitors.FileVisitor:
        return self.FileVisitor(self, partition_values)

    def to_metadata(self) -> MicroPartition:
        col_name = "add_action"
        if len(self.add_actions) == 0:
            return MicroPartition.empty(_get_schema_from_dict({col_name: DataType.python()}))
        return MicroPartition.from_pydict({col_name: self.add_actions})


def convert_pa_schema_to_delta(schema: pa.Schema, large_dtypes: bool) -> pa.Schema:
    import deltalake
    from packaging.version import parse

    deltalake_version = parse(deltalake.__version__)

    if deltalake_version < parse("0.19.0"):
        from deltalake.schema import _convert_pa_schema_to_delta

        return _convert_pa_schema_to_delta(schema, large_dtypes=large_dtypes)
    elif deltalake_version < parse("1.0.0"):
        from deltalake.schema import ArrowSchemaConversionMode, _convert_pa_schema_to_delta

        schema_conversion_mode = ArrowSchemaConversionMode.LARGE if large_dtypes else ArrowSchemaConversionMode.NORMAL
        return _convert_pa_schema_to_delta(schema, schema_conversion_mode=schema_conversion_mode)
    else:
        # deltalake>=1.0.0 passes through Arrow data types without modification
        # https://delta-io.github.io/delta-rs/upgrade-guides/guide-1.0.0/#large_dtypes-removed
        return schema


def create_table_with_add_actions(
    table_uri: str,
    schema: pa.Schema,
    add_actions: list[AddAction],
    mode: str,
    partition_by: list[str],
    name: str | None,
    description: str | None,
    configuration: Mapping[str, str | None] | None,
    storage_options: dict[str, str] | None,
    custom_metadata: dict[str, str] | None,
) -> None:
    import deltalake
    from packaging.version import parse

    if parse(deltalake.__version__) < parse("1.0.0"):
        from deltalake.writer import write_deltalake_pyarrow

        return write_deltalake_pyarrow(
            table_uri,
            schema,
            add_actions,
            mode,
            partition_by,
            name,
            description,
            configuration,
            storage_options,
            custom_metadata,
        )
    else:
        from deltalake import CommitProperties
        from deltalake.transaction import create_table_with_add_actions

        return create_table_with_add_actions(
            table_uri,
            deltalake.Schema.from_arrow(schema),
            add_actions,
            mode,
            partition_by,
            name,
            description,
            configuration,
            storage_options,
            CommitProperties(custom_metadata=custom_metadata),
        )


__all__ = ["AddAction"]
