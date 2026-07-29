from __future__ import annotations

import pathlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from daft import context, runners
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, McapSourceConfig, StorageConfig, io_glob
from daft.datatype import DataType
from daft.dependencies import pafs
from daft.filesystem import _resolve_paths_and_filesystem, canonicalize_protocol, get_protocol_from_path
from daft.io.source import DataSource, DataSourceTask
from daft.logical.schema import Schema

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from daft import DataFrame
    from daft.io.pushdowns import Pushdowns


TopicToStartTime = dict[str, int]
TopicStartTimeResolver = Callable[[str], TopicToStartTime]


@dataclass(frozen=True)
class _McapFile:
    path: str
    size_bytes: int | None


def _known_size(size: int | None) -> int | None:
    return size if size is not None and size >= 0 else None


def normalize_storage_path(
    path: str,
    io_config: IOConfig | None = None,
    source_protocol: str | None = None,
) -> str:
    """Add the configured storage protocol to protocol-less remote paths."""
    protocol = get_protocol_from_path(path)
    if protocol != "file":
        return path

    # Filesystem discovery strips object-store schemes from FileInfo paths.
    # Carry the protocol from the user's input so a local path is never
    # mistaken for S3 merely because every default IOConfig has an s3 field.
    if source_protocol is not None:
        if source_protocol == "file":
            return path
        return f"{source_protocol}://{path.lstrip('/')}"

    # Compatibility fallback for direct callers of this helper.
    if io_config:
        if io_config.s3:
            return f"s3://{path.lstrip('/')}"
        if io_config.azure:
            return f"abfs://{path.lstrip('/')}"
        if io_config.gcs:
            return f"gs://{path.lstrip('/')}"

    return path


def _list_files_with_metadata(
    root_dir: str | pathlib.Path,
    io_config: IOConfig | None,
    resolved_path: str | None = None,
    fs: pafs.FileSystem | None = None,
) -> list[_McapFile]:
    if isinstance(root_dir, pathlib.Path):
        root_dir = str(root_dir)

    # TODO: Remove this special case once all discovery uses the native IO
    # layer. It preserves the existing Hugging Face path behavior.
    if get_protocol_from_path(root_dir) == "hf":
        glob_path = root_dir if "*" in root_dir else root_dir.rstrip("/")
        if not glob_path.endswith(".mcap"):
            glob_path = f"{glob_path}/**/*.mcap" if "**" not in glob_path else glob_path
        files = io_glob(glob_path, io_config=io_config)
        return [
            _McapFile(path=file["path"], size_bytes=_known_size(file.get("size")))
            for file in files
            if file["type"] == "File"
        ]

    if resolved_path is None or fs is None:
        [resolved_path], fs = _resolve_paths_and_filesystem(root_dir, io_config=io_config)

    try:
        file_info = fs.get_file_info(resolved_path)
        if file_info.type == pafs.FileType.File:
            return [_McapFile(path=resolved_path, size_bytes=_known_size(file_info.size))]
    except FileNotFoundError:
        return []

    selector = pafs.FileSelector(resolved_path, recursive=True)
    try:
        file_infos = fs.get_file_info(selector)
    except NotADirectoryError:
        return [_McapFile(path=resolved_path, size_bytes=None)]
    except FileNotFoundError:
        return []

    return [
        _McapFile(path=file_info.path, size_bytes=_known_size(file_info.size))
        for file_info in file_infos
        if file_info.type == pafs.FileType.File
    ]


def list_files(
    root_dir: str | pathlib.Path,
    io_config: IOConfig | None,
    resolved_path: str | None = None,
    fs: pafs.FileSystem | None = None,
) -> list[str]:
    """List paths using the historical MCAP discovery semantics."""
    return [
        file.path
        for file in _list_files_with_metadata(
            root_dir,
            io_config,
            resolved_path=resolved_path,
            fs=fs,
        )
    ]


class MCAPSource(DataSource):
    def __init__(
        self,
        file_path: str | pathlib.Path,
        start_time: int | None = None,
        end_time: int | None = None,
        topics: list[str] | None = None,
        batch_size: int = 1000,
        io_config: IOConfig | None = None,
        topic_start_time_resolver: TopicStartTimeResolver | None = None,
        storage_config: StorageConfig | None = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {batch_size}")

        self._start_time = start_time
        self._end_time = end_time
        self._topics = topics
        self._batch_size = batch_size
        self._topic_start_time_resolver = topic_start_time_resolver
        self._io_config = io_config
        self._storage_config = storage_config or StorageConfig(True, io_config)
        source_protocol = canonicalize_protocol(get_protocol_from_path(str(file_path)))
        self._files = [
            _McapFile(
                path=normalize_storage_path(file.path, io_config, source_protocol),
                size_bytes=file.size_bytes,
            )
            for file in _list_files_with_metadata(file_path, io_config)
        ]

        if not self._files:
            raise FileNotFoundError(f"Path not found: {file_path}")

        self._schema = self._infer_schema()

    @property
    def name(self) -> str:
        return "MCAPSource"

    @property
    def schema(self) -> Schema:
        return self._schema

    def display_name(self) -> str:
        paths = [file.path for file in self._files]
        return f"MCAPSource({paths}, start_time={self._start_time}, end_time={self._end_time}, topics={self._topics})"

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
        ]

    @staticmethod
    def _infer_schema() -> Schema:
        return Schema.from_field_name_and_types(
            [
                ("source_path", DataType.string()),
                ("topic", DataType.string()),
                ("log_time", DataType.uint64()),
                ("publish_time", DataType.uint64()),
                ("sequence", DataType.uint32()),
                ("data", DataType.binary()),
            ]
        )

    def _task(
        self,
        file: _McapFile,
        pushdowns: Pushdowns,
        *,
        start_time: int | None,
        topics: list[str] | None,
    ) -> DataSourceTask:
        return DataSourceTask.mcap(
            path=file.path,
            schema=self._schema,
            mcap_config=McapSourceConfig(
                batch_size=self._batch_size,
                start_time=start_time,
                end_time=self._end_time,
                topics=topics,
            ),
            pushdowns=pushdowns,
            size_bytes=file.size_bytes,
            storage_config=self._storage_config,
        )

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        for file in self._files:
            keyframes: dict[str, int] | None = None
            if self._topic_start_time_resolver is not None:
                try:
                    keyframes = self._topic_start_time_resolver(file.path)
                except Exception:
                    keyframes = None

            if not keyframes:
                yield self._task(
                    file,
                    pushdowns,
                    start_time=self._start_time,
                    topics=self._topics,
                )
                continue

            topics = list(keyframes) if self._topics is None else self._topics
            for topic in topics:
                start_time = self._start_time
                if (keyframe_time := keyframes.get(topic)) is not None:
                    start_time = keyframe_time if start_time is None else max(start_time, keyframe_time)

                if self._end_time is not None and start_time is not None and start_time >= self._end_time:
                    continue

                yield self._task(
                    file,
                    pushdowns,
                    start_time=start_time,
                    topics=[topic],
                )


@PublicAPI
def read_mcap(
    path: str,
    io_config: IOConfig | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
    topics: list[str] | None = None,
    batch_size: int = 1000,
    topic_start_time_resolver: TopicStartTimeResolver | None = None,
) -> DataFrame:
    """Read raw messages from one or more MCAP files.

    Args:
        path: MCAP file or directory path.
        io_config: Configuration for storage credentials and native I/O.
        start_time: Inclusive non-negative lower bound for ``message.log_time``.
        end_time: Exclusive non-negative upper bound for ``message.log_time``.
        topics: Topic names to include.
        batch_size: Number of messages decoded per native record batch.
        topic_start_time_resolver: Optional per-file callback returning non-negative
            topic start times. Each result fans out into one native task per topic,
            using ``max(start_time, resolved_start_time)``.

    Warning:
        MCAP timestamps use the format's native unsigned 64-bit representation. ``start_time``,
        ``end_time``, and times returned by ``topic_start_time_resolver`` must be between 0 and
        ``2**64 - 1``. Negative time values accepted by earlier Daft versions now raise
        ``OverflowError`` because the output schema uses ``uint64`` timestamps instead of ``int64``.

    Returns:
        A DataFrame with ``source_path``, ``topic``, ``log_time``,
        ``publish_time``, ``sequence``, and raw binary ``data`` columns.
    """
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    multithreaded_io = runners.get_or_create_runner().name != "ray"
    storage_config = StorageConfig(multithreaded_io, io_config)

    return MCAPSource(
        file_path=path,
        start_time=start_time,
        end_time=end_time,
        topics=topics,
        batch_size=batch_size,
        io_config=io_config,
        topic_start_time_resolver=topic_start_time_resolver,
        storage_config=storage_config,
    ).read()
