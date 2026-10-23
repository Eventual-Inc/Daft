from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from daft.api_annotations import PublicAPI
from daft.context import get_context
from daft.dependencies import pa, pads
from daft.expressions import ExpressionsProjection, col, lit
from daft.file import open_file
from daft.filesystem import glob_path_with_stats
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Generator

    from daft import DataFrame
    from daft.daft import IOConfig
    from daft.io.pushdowns import Pushdowns


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
) -> Generator[RecordBatch, None, None]:
    with open_file(path, "rb", io_config=io_config) as file:
        try:
            fragment = pads.OrcFileFormat().make_fragment(file)
            physical_schema = fragment.physical_schema
            columns = [name for name in schema.names if name in physical_schema.names]
            target_schema = Schema.from_pyarrow_schema(schema)
            projection = ExpressionsProjection(
                [
                    (col(field.name) if field.name in columns else lit(None)).cast(field.dtype).alias(field.name)
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
                if record_batch.schema() != target_schema:
                    record_batch = record_batch.eval_expression_list(projection)
                yield record_batch
        except OSError as error:
            raise OSError(f"Unable to read ORC file {path!r}: {error}") from error


@PublicAPI
def read_orc(
    path: str | list[str],
    io_config: IOConfig | None = None,
    batch_size: int = 128 * 1024,
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
    return OrcSource(path, io_config=io_config, batch_size=batch_size).read()


class OrcSource(DataSource):
    def __init__(self, path: str | list[str], io_config: IOConfig | None, batch_size: int) -> None:
        self._paths = _resolve_orc_paths(path, io_config)
        self._io_config = io_config
        self._batch_size = batch_size
        self._arrow_schema = _infer_orc_schema(self._paths[0], io_config)
        self._schema = Schema.from_pyarrow_schema(self._arrow_schema)

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
            yield OrcSourceTask(path, schema, self._io_config, self._batch_size)


@dataclass
class OrcSourceTask(DataSourceTask):
    _path: str
    _arrow_schema: pa.Schema
    _io_config: IOConfig | None
    _batch_size: int

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(self._arrow_schema)

    async def read(self) -> AsyncIterator[RecordBatch]:
        batches = _iter_orc_batches(self._path, self._arrow_schema, self._io_config, self._batch_size)
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
