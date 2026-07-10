from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from daft.context import get_context
from daft.datatype import DataType
from daft.filesystem import get_protocol_from_path
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
    try:
        return arrow_table.cast(arrow_batch)
    except Exception as exc:  # pa.ArrowInvalid and friends
        unsigned = [f.name for f in arrow_table.schema if str(f.type).startswith("uint")]
        if unsigned:
            raise ValueError(
                f"Failed to write column(s) {unsigned} to Delta Lake. Delta has no "
                f"unsigned integer types; uint64 values above 2**63-1 cannot be "
                f"represented. Cast the column explicitly (e.g. to decimal) before "
                f"writing. Original error: {exc}"
            ) from exc
        raise


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


# Codecs PyArrow's parquet writer can encode. `lz4` is included: with a single writer
# there is no ambiguity about which parquet codec it maps to.
_DELTA_COMPRESSION_CODECS = frozenset({"none", "snappy", "gzip", "brotli", "lz4", "zstd"})

# `uncompressed` is a spelling of `none`, not a distinct codec. PyArrow's writer rejects
# the string but accepts `none`; `write_parquet` documents `uncompressed`, so we keep it
# working here rather than gratuitously diverging.
_DELTA_COMPRESSION_ALIASES = {"uncompressed": "none"}

# Codecs the parquet format defines but PyArrow cannot encode. `lzo` is the treacherous
# one: `pq.ParquetWriter(compression="lzo")` constructs successfully and raises on the
# first `write_table`, so a zero-row capability probe reports it as supported.
_DELTA_COMPRESSION_REJECTED = {
    "lz4_raw": "PyArrow's parquet writer cannot encode it. Use 'lz4' instead.",
    "lzo": (
        "PyArrow's C++ Parquet implementation does not support LZO encoding "
        "(it fails on the first write, not at writer construction). "
        "Use 'snappy' or 'zstd' instead."
    ),
}


def normalize_delta_compression(compression: str) -> str:
    """Validate a Delta write compression codec and return its canonical name.

    Case-insensitive; surrounding whitespace is ignored. Maps ``uncompressed`` to
    ``none``. Raises ``ValueError`` both for codecs PyArrow cannot encode and for
    unrecognized codec names, so the failure surfaces at call time rather than mid-write.
    """
    codec = compression.strip().lower()
    codec = _DELTA_COMPRESSION_ALIASES.get(codec, codec)

    if codec in _DELTA_COMPRESSION_REJECTED:
        raise ValueError(
            f"compression={compression!r} is not supported for Delta writes: "
            f"{_DELTA_COMPRESSION_REJECTED[codec]}"
        )
    if codec not in _DELTA_COMPRESSION_CODECS:
        accepted = ", ".join(sorted(_DELTA_COMPRESSION_CODECS | {"uncompressed"}))
        raise ValueError(
            f"Unsupported compression codec {compression!r} for Delta writes. "
            f"Accepted codecs: {accepted}."
        )
    return codec


def make_deltalake_fs(path: str, io_config: IOConfig | None = None) -> pafs.PyFileSystem:
    from deltalake.fs import DeltaStorageHandler
    from pyarrow.fs import PyFileSystem

    from daft.io.object_store_options import io_config_to_storage_options

    io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_options = io_config_to_storage_options(io_config, path)

    protocol = get_protocol_from_path(path)
    if protocol == "file":
        local_path = urlparse(path).path if path.startswith("file://") else path
        os.makedirs(local_path, exist_ok=True)

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
            size = written_file.size

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


def _normalize_delta_timestamp_type(dtype: pa.DataType) -> pa.DataType:
    """Rewrite tz-aware timestamps to use the ``UTC`` timezone label, recursing into nested types.

    Delta Lake stores every timezone-aware timestamp as a UTC instant, and
    deltalake>=1.0.0's ``Schema.from_arrow`` only accepts the literal timezone
    ``"UTC"`` -- it rejects the fixed-offset spelling (``"+00:00"``) that Daft's
    ``DataType.to_arrow_dtype()`` emits, as well as any named zone. The int64
    values are unchanged (same absolute instant), so relabeling the timezone to
    ``UTC`` is a lossless conversion, not a reinterpretation.
    """
    from daft.dependencies import pa

    def norm_field(field: pa.Field) -> pa.Field:
        return field.with_type(_normalize_delta_timestamp_type(field.type))

    if pa.types.is_timestamp(dtype):
        if dtype.tz is not None and dtype.tz != "UTC":
            return pa.timestamp(dtype.unit, tz="UTC")
        return dtype
    if pa.types.is_struct(dtype):
        return pa.struct([norm_field(dtype.field(i)) for i in range(dtype.num_fields)])
    if pa.types.is_large_list(dtype):
        return pa.large_list(norm_field(dtype.value_field))
    if pa.types.is_fixed_size_list(dtype):
        return pa.list_(norm_field(dtype.value_field), dtype.list_size)
    if pa.types.is_list(dtype):
        return pa.list_(norm_field(dtype.value_field))
    if pa.types.is_map(dtype):
        return pa.map_(norm_field(dtype.key_field), norm_field(dtype.item_field), keys_sorted=dtype.keys_sorted)
    return dtype


def _normalize_delta_schema_timestamps(schema: pa.Schema) -> pa.Schema:
    """Apply :func:`_normalize_delta_timestamp_type` to every field of a schema."""
    from daft.dependencies import pa

    normalized = [field.with_type(_normalize_delta_timestamp_type(field.type)) for field in schema]
    return pa.schema(normalized, metadata=schema.metadata)


# Delta Lake has no unsigned types: delta-rs maps uint8->byte(int8), uint16->short(int16),
# uint32->integer(int32), uint64->long(int64). Any value above the corresponding SIGNED
# maximum commits and then cannot be read. Widen to the next signed type that holds every
# value. uint64 has no lossless target; values above i64::MAX raise at cast time.
_DELTA_UNSIGNED_WIDENING = [
    ("uint8", "int16"),
    ("uint16", "int32"),
    ("uint32", "int64"),
    ("uint64", "int64"),
]


def _widen_unsigned_type(dtype: pa.DataType) -> pa.DataType:
    """Rewrite unsigned integer types to a signed type Delta can represent.

    Recurses into nested types. uint8/uint16/uint32 widen losslessly. uint64 widens to
    int64, which is lossless up to i64::MAX; beyond that the table cast raises, which is
    strictly better than committing a table nobody can read.
    """
    from daft.dependencies import pa

    def widen_field(field: pa.Field) -> pa.Field:
        return field.with_type(_widen_unsigned_type(field.type))

    if pa.types.is_uint8(dtype):
        return pa.int16()
    if pa.types.is_uint16(dtype):
        return pa.int32()
    if pa.types.is_uint32(dtype) or pa.types.is_uint64(dtype):
        return pa.int64()
    if pa.types.is_struct(dtype):
        return pa.struct([widen_field(dtype.field(i)) for i in range(dtype.num_fields)])
    if pa.types.is_large_list(dtype):
        return pa.large_list(widen_field(dtype.value_field))
    if pa.types.is_fixed_size_list(dtype):
        return pa.list_(widen_field(dtype.value_field), dtype.list_size)
    if pa.types.is_list(dtype):
        return pa.list_(widen_field(dtype.value_field))
    if pa.types.is_map(dtype):
        return pa.map_(
            widen_field(dtype.key_field),
            widen_field(dtype.item_field),
            keys_sorted=dtype.keys_sorted,
        )
    return dtype


def _widen_unsigned_schema(schema: pa.Schema) -> pa.Schema:
    """Apply :func:`_widen_unsigned_type` to every field of a schema."""
    from daft.dependencies import pa

    widened = [field.with_type(_widen_unsigned_type(field.type)) for field in schema]
    return pa.schema(widened, metadata=schema.metadata)


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
        # deltalake>=1.0.0 passes Arrow data types through without modification, EXCEPT
        # that its Schema.from_arrow rejects tz-aware timestamps whose timezone is not the
        # literal "UTC" (e.g. the "+00:00" that Daft emits). The pre-1.0.0 converters above
        # normalized this for us; restore that normalization here so tz-aware timestamps
        # commit correctly. https://delta-io.github.io/delta-rs/upgrade-guides/guide-1.0.0/#large_dtypes-removed
        # Delta also has no unsigned integer types, so widen those too (see
        # _widen_unsigned_type).
        return _widen_unsigned_schema(_normalize_delta_schema_timestamps(schema))


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
