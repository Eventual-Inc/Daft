from __future__ import annotations

import asyncio
import os
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal
from urllib.parse import unquote, urlsplit

from daft.api_annotations import PublicAPI
from daft.context import get_context
from daft.datatype import DataType
from daft.dependencies import pa, pads
from daft.exceptions import DaftCoreException
from daft.expressions import ExpressionsProjection, col, lit
from daft.file import open_file
from daft.filesystem import glob_path_with_stats
from daft.io.partitioning import PartitionField
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema
from daft.series import Series

if TYPE_CHECKING:
    from collections.abc import Generator

    from daft import DataFrame
    from daft.daft import IOConfig
    from daft.io.pushdowns import Pushdowns


_HIVE_INTEGER = re.compile(r"[+-]?[0-9]+")
_HIVE_FLOAT = re.compile(
    r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?|[+-]?(?:inf(?:inity)?|nan)", re.IGNORECASE | re.ASCII
)
_HIVE_TIME = re.compile(
    r"\s*(?P<hour>[0-9]{1,2})\s*:\s*(?P<minute>[0-9]{1,2})"
    r"(?:\s*:\s*(?P<second>[0-9]{1,2})(?:\.(?P<fraction>[0-9]+))?)?\s*"
)
_HIVE_TIMESTAMP = re.compile(
    r"\s*[+-]?[0-9]+\s*-\s*[0-9]{1,2}\s*-\s*[0-9]{1,2}(?P<separator>[Tt]|\s*)"
    r"\s*[0-9]{1,2}\s*:\s*[0-9]{1,2}\s*:\s*(?P<second>[0-9]{1,2})(?:\.(?P<fraction>[0-9]+))?"
    r"(?:\s*(?P<offset>[Zz]|[Uu][Tt][Cc]|[+\-\u2212][0-9]{2}[:\s]*[0-9]{2}))?"
)
_HIVE_TIME_FACTORS = {"s": 1, "ms": 1000, "us": 1000000, "ns": 1000000000}


def _parse_orc_hive_partitions(path: str) -> dict[str, str | None]:
    # Split before decoding so escaped separators stay inside their key/value.
    directory = re.split(r"[?\n]", path, maxsplit=1)[0]
    partitions: dict[str, str | None] = {}
    for component in re.split(r"[/\\]", directory)[:-1]:
        if component.count("=") != 1:
            continue
        key, value = component.split("=", 1)
        if not key:
            continue
        key = unquote(key, errors="strict")
        value = unquote(value, errors="strict")
        partitions[key] = None if value in {"", "__HIVE_DEFAULT_PARTITION__"} else value
    return partitions


def _hive_time_unit(fraction: str | None) -> Literal["s", "ms", "us", "ns"]:
    nanos = int(((fraction or "") + "000000000")[:9])
    if nanos == 0:
        return "s"
    if nanos % 1000000 == 0:
        return "ms"
    return "us" if nanos % 1000 == 0 else "ns"


def _parse_hive_temporal(value: str) -> tuple[pa.DataType, int] | None:
    source = Series.from_pylist([value])
    for date_format in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            days = source.str.to_date(date_format).cast(DataType.int32()).to_pylist()[0]
        except DaftCoreException as error:
            if "Error in to_date: failed to parse date" not in str(error):
                raise
        else:
            return pa.date32(), days

    time_match = _HIVE_TIME.fullmatch(value)
    timestamp_match = _HIVE_TIMESTAMP.fullmatch(value)
    if time_match is None and timestamp_match is None:
        return None
    if time_match is not None:
        fraction = time_match["fraction"]
        time_unit: Literal["us", "ns"] = "ns" if _hive_time_unit(fraction) == "ns" else "us"
        unit: Literal["s", "ms", "us", "ns"] = time_unit
        source = Series.from_pylist([f"1970-01-01T{value}"])
        formats: tuple[str, ...] = (
            "%Y-%m-%dT%H :%M :%S%.f " if time_match["second"] is not None else "%Y-%m-%dT%H :%M ",
        )
        dtype: pa.DataType = pa.time64(time_unit)
    else:
        assert timestamp_match is not None
        separator = timestamp_match["separator"]
        fraction = timestamp_match["fraction"]
        offset = timestamp_match["offset"]
        unit = _hive_time_unit(fraction)
        # Parse in microseconds so calendar validation also works outside the
        # nanosecond range. Restore sub-microsecond digits before conversion.
        timezone = None
        if offset is not None:
            formats = ("%+", "%Y-%m-%d %H:%M:%S%.f%:z")
            if offset.lower() in {"z", "utc"}:
                timezone = "+00:00"
            else:
                # Match inference.rs's signed integer offset calculation.
                offset = re.sub(r"[\s:]", "", offset).replace("\u2212", "-")
                minutes = (int(offset[1:3]) * 60 + int(offset[3:5])) * (-1 if offset[0] == "-" else 1)
                hours = int(minutes / 60)
                timezone = f"{hours:+03}:{minutes - hours * 60:02}"
        else:
            # Lowercase 't' is accepted by RFC3339, but not by the naive
            # formats in ALL_NAIVE_TIMESTAMP_FMTS.
            if separator == "t":
                return None
            formats = ("%Y-%m-%dT%H:%M:%S%.f",) if separator == "T" else ("%Y-%m-%d %H:%M:%S%.f",)
        dtype = pa.timestamp(unit, timezone)
    parsed = None
    for format_string in formats:
        try:
            parsed = source.str.to_datetime(format_string)
        except DaftCoreException as error:
            if "Error in to_datetime: failed to parse datetime" not in str(error):
                raise
        else:
            break
    if parsed is None:
        return None
    ticks = parsed.cast(DataType.int64()).to_pylist()[0]
    nanos = ticks * 1000 + int(((fraction or "") + "000000000")[:9]) % 1000
    ticks = nanos // (1000000000 // _HIVE_TIME_FACTORS[unit])
    if timestamp_match is not None and timestamp_match["second"] == "60" and unit == "s":
        # Chrono timestamp() excludes the leap-second nanoseconds, whereas
        # timestamp_millis()/micros()/nanos() include them.
        ticks -= 1
    return dtype, ticks


def _parse_hive_integer(value: str) -> int | None:
    if _HIVE_INTEGER.fullmatch(value) is None:
        return None
    digits = value.lstrip("+-").lstrip("0") or "0"
    if len(digits) > 19:
        return None
    integer = int(digits) * (-1 if value.startswith("-") else 1)
    return integer if -(2**63) <= integer < 2**63 else None


def _infer_hive_type(value: str | None) -> pa.DataType:
    if value is None:
        return pa.string()
    if value.lower() in {"true", "false"}:
        return pa.bool_()
    if _parse_hive_integer(value) is not None:
        return pa.int64()
    if _HIVE_FLOAT.fullmatch(value):
        return pa.float64()
    temporal = _parse_hive_temporal(value)
    return temporal[0] if temporal is not None else pa.string()


def _convert_hive_value(value: str | None, dtype: pa.DataType) -> bool | int | float | str | None:
    if value is None:
        return None
    if pa.types.is_string(dtype):
        return value
    if pa.types.is_boolean(dtype):
        return value.lower() == "true" if value.lower() in {"true", "false"} else None
    if pa.types.is_int64(dtype):
        return _parse_hive_integer(value)
    if pa.types.is_float64(dtype):
        return float(value) if _HIVE_FLOAT.fullmatch(value) else None
    if pa.types.is_date32(dtype) or pa.types.is_time64(dtype) or pa.types.is_timestamp(dtype):
        if (
            pa.types.is_timestamp(dtype)
            and dtype.tz is not None
            and dtype.tz.startswith(("+", "-"))
            and re.fullmatch(r"[+-](?:[01][0-9]|2[0-3]):[0-5][0-9]", dtype.tz) is None
        ):
            raise ValueError(f"Invalid ORC Hive partition timezone: {dtype.tz}")
        temporal = _parse_hive_temporal(value)
        if temporal is None:
            return None
        parsed_dtype, ticks = temporal
        if pa.types.is_date32(dtype):
            return ticks if pa.types.is_date32(parsed_dtype) else None
        if pa.types.is_time64(dtype) != pa.types.is_time64(parsed_dtype):
            return None
        if pa.types.is_timestamp(dtype) and (
            not pa.types.is_timestamp(parsed_dtype) or (dtype.tz is None) != (parsed_dtype.tz is None)
        ):
            return None
        if not isinstance(parsed_dtype, (pa.Time64Type, pa.TimestampType)):
            return None
        ticks = ticks * _HIVE_TIME_FACTORS[dtype.unit] // _HIVE_TIME_FACTORS[parsed_dtype.unit]
        timestamp_match = _HIVE_TIMESTAMP.fullmatch(value)
        if pa.types.is_timestamp(dtype) and timestamp_match is not None and timestamp_match["second"] == "60":
            if parsed_dtype.unit == "s" and dtype.unit != "s":
                ticks += _HIVE_TIME_FACTORS[dtype.unit]
            elif parsed_dtype.unit != "s" and dtype.unit == "s":
                ticks -= 1
        return ticks if -(2**63) <= ticks < 2**63 else None
    raise ValueError(f"Unsupported ORC Hive partition type: {dtype}")


def _is_same_file(path: str, file_path: str) -> bool:
    parsed = urlsplit(path)
    if parsed.scheme in {"", "file"} or os.path.isabs(path):
        local_path = path.removeprefix("file:").removeprefix("//") if parsed.scheme == "file" else path
        resolved_path = file_path.removeprefix("file://")
        if os.name == "nt":
            if local_path.startswith("/") and local_path[2:3] == ":":
                local_path = local_path[1:]
            if resolved_path.startswith("/") and resolved_path[2:3] == ":":
                resolved_path = resolved_path[1:]
        return os.path.normcase(os.path.abspath(local_path)) == os.path.normcase(os.path.abspath(resolved_path))
    return path == file_path


def _resolve_orc_paths(path: str | list[str], io_config: IOConfig | None) -> list[str]:
    paths = [path] if isinstance(path, str) else path
    if not paths:
        raise ValueError("Cannot read DataFrame from empty list of ORC filepaths")
    if any(input_path == "" for input_path in paths):
        raise ValueError("Cannot read DataFrame from an empty ORC filepath")

    resolved_paths: list[str] = []
    seen: set[str] = set()
    for input_path in paths:
        parsed = urlsplit(input_path)
        if not parsed.scheme or os.path.isabs(input_path):
            input_path = os.path.abspath(os.path.expanduser(input_path))
        # For paths without glob characters, native I/O tries an exact file
        # before listing a directory, including files without an .orc extension.
        file_paths = glob_path_with_stats(input_path, file_format=None, io_config=io_config).file_paths
        is_glob = any(character in input_path for character in "*?[{")
        is_file = len(file_paths) == 1 and _is_same_file(input_path, file_paths[0])
        if not is_glob and not is_file:
            file_paths = glob_path_with_stats(
                f"{input_path.rstrip('/')}/**/*.orc", file_format=None, io_config=io_config
            ).file_paths
        if not file_paths:
            raise FileNotFoundError(f"No ORC files found for {input_path!r}")
        for file_path in file_paths:
            if file_path not in seen:
                seen.add(file_path)
                resolved_paths.append(file_path)
    return resolved_paths


def _infer_orc_schema(path: str, io_config: IOConfig | None) -> pa.Schema:
    with open_file(path, "rb", io_config=io_config) as file:
        try:
            schema: pa.Schema = pads.OrcFileFormat().inspect(file)
        except OSError as error:
            raise OSError(f"Unable to infer ORC schema for {path!r}: {error}") from error
        return schema


def _iter_orc_batches(
    path: str,
    schema: pa.Schema,
    io_config: IOConfig | None,
    batch_size: int,
    partition_values: dict[str, bool | int | float | str | None] | None = None,
) -> Generator[RecordBatch, None, None]:
    with open_file(path, "rb", io_config=io_config) as file:
        try:
            fragment = pads.OrcFileFormat().make_fragment(file)
            physical_schema = fragment.physical_schema
            partition_values = {} if partition_values is None else partition_values
            columns = [name for name in schema.names if name in physical_schema.names and name not in partition_values]
            target_schema = Schema.from_pyarrow_schema(schema)
            projection = ExpressionsProjection(
                [
                    (
                        lit(partition_values[field.name])
                        if field.name in partition_values
                        else col(field.name)
                        if field.name in columns
                        else lit(None)
                    )
                    .cast(field.dtype)
                    .alias(field.name)
                    for field in target_schema
                ]
            )
            scanner = pads.Scanner.from_fragment(
                fragment,
                schema=physical_schema,
                columns=columns,
                batch_size=batch_size,
                batch_readahead=0,
                use_threads=False,
            )
            for batch in scanner.to_batches():
                if batch.num_columns == 0:
                    # Preserve cardinality for empty projections (e.g. count).
                    record_batch = RecordBatch._from_series([], num_rows=batch.num_rows)
                else:
                    record_batch = RecordBatch.from_arrow_record_batches([batch], batch.schema)
                # Match native file scans: cast and fill through Daft rather
                # than Arrow, whose nested schema evolution varies by version.
                if partition_values or record_batch.schema() != target_schema:
                    record_batch = record_batch.eval_expression_list(projection)
                yield record_batch
        except OSError as error:
            raise OSError(f"Unable to read ORC file {path!r}: {error}") from error


@PublicAPI
def read_orc(
    path: str | list[str],
    io_config: IOConfig | None = None,
    batch_size: int = 128 * 1024,
    *,
    hive_partitioning: bool = False,
) -> DataFrame:
    """Creates a DataFrame from ORC file(s).

    Args:
        path: Path to an ORC file, directory, glob, or list of paths. Directories
            are searched recursively for ``*.orc`` files. Supports remote URLs
            to object stores such as ``s3://`` or ``gs://``. Uses native glob
            syntax; glob metacharacters in literal filenames must be escaped.
        io_config: Configuration for the native file I/O backend. Defaults to
            the planning context's default I/O configuration.
        batch_size: Maximum number of rows yielded per record batch. Defaults
            to 131072. This does not impose a fixed memory limit.
        hive_partitioning: Read partition columns from Hive-style ``key=value``
            directories and prune files using partition filters. Defaults to False.
            Keys and types are inferred from the first matched file. Missing keys
            become null, and directory values override same-named physical columns.

    Returns:
        DataFrame: parsed DataFrame.

    Note:
        The schema is inferred from the first matched file. Later files are
        aligned to that schema: missing fields become nulls and extra fields
        are excluded. Conversions follow Daft's rules, as with Parquet reads:
        unsupported conversions raise an error, while some invalid values
        (such as invalid numeric strings) become nulls. An empty ORC file with a valid
        schema is supported.
        Each file is read by one task; stripes are not split into separate
        distributed tasks. Filters and limits use Daft's existing execution
        operators, without ORC-native predicate pruning.
        A limit does not guarantee early termination of file reads. The shared
        Python source bridge can continue reading and buffering batches after
        the returned-row limit is reached.

    Examples:
        Read ORC files from a local path:
        >>> df = daft.read_orc("/path/to/file.orc")  # doctest: +SKIP
        >>> df = daft.read_orc("/path/to/directory")  # doctest: +SKIP
        >>> df = daft.read_orc("/path/to/files-*.orc")  # doctest: +SKIP
    """
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError(f"batch_size must be a positive integer, received {batch_size!r}")

    io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
    return OrcSource(path, io_config=io_config, batch_size=batch_size, hive_partitioning=hive_partitioning).read()


class OrcSource(DataSource):
    def __init__(
        self, path: str | list[str], io_config: IOConfig | None, batch_size: int, hive_partitioning: bool = False
    ) -> None:
        self._paths = _resolve_orc_paths(path, io_config)
        self._io_config = io_config
        self._batch_size = batch_size
        self._arrow_schema = _infer_orc_schema(self._paths[0], io_config)
        partitions = _parse_orc_hive_partitions(self._paths[0]) if hive_partitioning else {}
        self._partition_schema = pa.schema(
            [pa.field(key, _infer_hive_type(value)) for key, value in partitions.items()]
        )
        partition_fields = {field.name: field for field in self._partition_schema}
        if partition_fields:
            self._arrow_schema = pa.schema(
                [partition_fields.get(field.name, field) for field in self._arrow_schema]
                + [field for field in self._partition_schema if field.name not in self._arrow_schema.names],
                metadata={key: value for key, value in (self._arrow_schema.metadata or {}).items()} or None,
            )
        self._schema = Schema.from_pyarrow_schema(self._arrow_schema)

    def get_partition_fields(self) -> list[PartitionField]:
        return [PartitionField.create(field) for field in Schema.from_pyarrow_schema(self._partition_schema)]

    @property
    def name(self) -> str:
        return "OrcSource"

    @property
    def schema(self) -> Schema:
        return self._schema

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[OrcSourceTask]:
        if pushdowns.columns is None:
            schema = self._arrow_schema
        else:
            required = set(pushdowns.columns) | pushdowns.filter_required_column_names()
            schema = pa.schema([field for field in self._arrow_schema if field.name in required])
        for path in self._paths:
            values: dict[str, bool | int | float | str | None] = {}
            if len(self._partition_schema) > 0:
                partitions = _parse_orc_hive_partitions(path)
                values = {
                    field.name: _convert_hive_value(partitions.get(field.name), field.type)
                    for field in self._partition_schema
                }
                constants = RecordBatch.from_pydict({key: [value] for key, value in values.items()})
                constants = constants.eval_expression_list(
                    ExpressionsProjection(
                        [
                            col(field.name).cast(field.dtype).alias(field.name)
                            for field in Schema.from_pyarrow_schema(self._partition_schema)
                        ]
                    )
                )
                if (
                    pushdowns.partition_filters is not None
                    and len(constants.filter(ExpressionsProjection([pushdowns.partition_filters]))) == 0
                ):
                    continue
            yield OrcSourceTask(path, schema, self._io_config, self._batch_size, values)


@dataclass
class OrcSourceTask(DataSourceTask):
    _path: str
    _arrow_schema: pa.Schema
    _io_config: IOConfig | None
    _batch_size: int
    _partition_values: dict[str, bool | int | float | str | None] = field(default_factory=dict)

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(self._arrow_schema)

    async def read(self) -> AsyncIterator[RecordBatch]:
        batches = _iter_orc_batches(
            self._path, self._arrow_schema, self._io_config, self._batch_size, self._partition_values
        )
        try:
            while True:
                pending = asyncio.create_task(asyncio.to_thread(next, batches, None))
                try:
                    batch = await asyncio.shield(pending)
                except asyncio.CancelledError:
                    # to_thread cannot interrupt a running next(). Wait before
                    # closing its generator and underlying file on another thread.
                    await asyncio.gather(pending, return_exceptions=True)
                    raise
                if batch is None:
                    break
                yield batch
        finally:
            await asyncio.to_thread(batches.close)
