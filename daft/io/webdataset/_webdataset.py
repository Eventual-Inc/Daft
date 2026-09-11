from __future__ import annotations

import asyncio
import json
import re
import tarfile
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from itertools import islice
from typing import TYPE_CHECKING

from daft.api_annotations import PublicAPI
from daft.context import get_context
from daft.daft import io_glob
from daft.datatype import DataType, MediaType
from daft.file import File, open_file
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema
from daft.series import Series

if TYPE_CHECKING:
    from daft import DataFrame
    from daft.daft import IOConfig
    from daft.io.pushdowns import Pushdowns


_SAMPLES_FOR_SCHEMA_INFERENCE = 5
_BASE_PLUS_EXTENSION = re.compile(r"^((?:.*/|)[^.]+)[.]([^/]*)$")
_ARCHIVE_METADATA = re.compile(r"__[^/]*__($|/)")

_IMAGE_EXTENSIONS = frozenset(
    {
        "apng",
        "avif",
        "bmp",
        "gif",
        "heic",
        "heif",
        "jfif",
        "jp2",
        "jpeg",
        "jpg",
        "jxl",
        "png",
        "tif",
        "tiff",
        "webp",
    }
)
_AUDIO_EXTENSIONS = frozenset(
    {
        "aac",
        "aiff",
        "au",
        "caf",
        "flac",
        "m4a",
        "mp3",
        "ogg",
        "opus",
        "wav",
        "wma",
    }
)
_VIDEO_EXTENSIONS = frozenset(
    {
        "avi",
        "m4v",
        "mkv",
        "mov",
        "mp4",
        "mpeg",
        "mpg",
        "ogv",
        "webm",
        "wmv",
    }
)
_JSON_EXTENSIONS = frozenset({"json", "jsn"})
_TEXT_EXTENSIONS = frozenset({"text", "transcript", "txt"})
_INTEGER_EXTENSIONS = frozenset({"cls", "cls2", "id", "index", "inx"})
_COMPRESSED_TAR_SUFFIXES = (".tar.bz2", ".tar.gz", ".tar.xz", ".tbz2", ".tgz", ".txz")
_END_OF_ITERATOR = object()


def _field_extension(field_name: str) -> str:
    return field_name.rsplit(".", 1)[-1].lower()


def _media_type(field_name: str) -> MediaType:
    extension = _field_extension(field_name)
    if extension in _IMAGE_EXTENSIONS:
        return MediaType.image()
    if extension in _AUDIO_EXTENSIONS:
        return MediaType.audio()
    if extension in _VIDEO_EXTENSIONS:
        return MediaType.video()
    return MediaType.unknown()


def _is_eagerly_decoded(field_name: str) -> bool:
    extension = _field_extension(field_name)
    return extension in _JSON_EXTENSIONS | _TEXT_EXTENSIONS | _INTEGER_EXTENSIONS


def _decode_member(archive: tarfile.TarFile, member: tarfile.TarInfo, field_name: str) -> object:
    extracted = archive.extractfile(member)
    if extracted is None:
        raise ValueError(f"Unable to read regular file {member.name!r} from WebDataset archive")

    data = extracted.read()
    extension = _field_extension(field_name)
    try:
        if extension in _JSON_EXTENSIONS:
            return json.loads(data)
        if extension in _TEXT_EXTENSIONS:
            return data.decode("utf-8")
        if extension in _INTEGER_EXTENSIONS:
            return int(data)
    except (UnicodeDecodeError, ValueError) as error:
        raise ValueError(f"Unable to decode WebDataset member {member.name!r}") from error

    raise AssertionError(f"No eager decoder registered for WebDataset field {field_name!r}")


def _member_value(
    archive: tarfile.TarFile,
    archive_path: str,
    member: tarfile.TarInfo,
    field_name: str,
    io_config: IOConfig | None,
) -> object:
    if _is_eagerly_decoded(field_name):
        return _decode_member(archive, member, field_name)

    return File(
        archive_path,
        io_config=io_config,
        media_type=_media_type(field_name),
        position=member.offset_data,
        size=member.size,
    )


def _iter_archive_samples(
    archive_path: str,
    io_config: IOConfig | None,
    selected_columns: set[str] | None = None,
    known_fields: set[str] | frozenset[str] | None = None,
) -> Iterator[dict[str, object]]:
    current_key: str | None = None
    current_sample: dict[str, object] | None = None
    seen_fields: set[str] = set()

    with open_file(archive_path, "rb", io_config=io_config) as file:
        try:
            with tarfile.open(fileobj=file, mode="r:") as archive:
                for member in archive:
                    if not member.isreg() or _ARCHIVE_METADATA.match(member.name):
                        continue

                    match = _BASE_PLUS_EXTENSION.match(member.name)
                    if match is None:
                        continue

                    sample_key, field_name = match.groups()
                    field_name = field_name.lower()

                    if known_fields is not None and field_name not in known_fields:
                        raise ValueError(
                            f"WebDataset field {field_name!r} in archive {archive_path!r} "
                            "was not present during schema inference. WebDataset shards must "
                            "use a consistent set of member suffixes."
                        )

                    if current_key != sample_key:
                        if current_sample is not None:
                            yield current_sample
                        current_key = sample_key
                        current_sample = {"__key__": sample_key, "__url__": archive_path}
                        seen_fields = set()

                    if field_name in seen_fields:
                        raise ValueError(
                            f"Duplicate WebDataset field {field_name!r} for sample {sample_key!r} "
                            f"in archive {archive_path!r}"
                        )
                    seen_fields.add(field_name)

                    if selected_columns is None or field_name in selected_columns:
                        assert current_sample is not None
                        current_sample[field_name] = _member_value(
                            archive,
                            archive_path,
                            member,
                            field_name,
                            io_config,
                        )
        except tarfile.ReadError as error:
            raise ValueError(
                f"Unable to read {archive_path!r} as an uncompressed TAR archive. "
                "Compressed WebDataset shards are not supported."
            ) from error

    if current_sample is not None:
        yield current_sample


def _field_dtype(field_name: str, samples: list[dict[str, object]]) -> DataType:
    if field_name in {"__key__", "__url__"}:
        return DataType.string()

    extension = _field_extension(field_name)
    if extension in _IMAGE_EXTENSIONS:
        return DataType.file(MediaType.image())
    if extension in _AUDIO_EXTENSIONS:
        return DataType.file(MediaType.audio())
    if extension in _VIDEO_EXTENSIONS:
        return DataType.file(MediaType.video())
    if extension in _TEXT_EXTENSIONS:
        return DataType.string()
    if extension in _INTEGER_EXTENSIONS:
        return DataType.int64()
    if extension in _JSON_EXTENSIONS:
        values = [sample.get(field_name) for sample in samples]
        return Series.from_pylist(values, name=field_name).datatype()
    return DataType.file()


def _infer_schema(archive_path: str, io_config: IOConfig | None) -> Schema:
    samples = list(
        islice(
            _iter_archive_samples(archive_path, io_config),
            _SAMPLES_FOR_SCHEMA_INFERENCE,
        )
    )
    if not samples:
        raise ValueError(f"WebDataset archive {archive_path!r} does not contain any valid samples")

    field_names = ["__key__", "__url__"]
    discovered = set(field_names)
    for sample in samples:
        for field_name in sample:
            if field_name not in discovered:
                discovered.add(field_name)
                field_names.append(field_name)

    return Schema.from_pydict({field_name: _field_dtype(field_name, samples) for field_name in field_names})


def _is_compressed_tar(path: str) -> bool:
    return path.lower().endswith(_COMPRESSED_TAR_SUFFIXES)


def _glob_pattern(path: str) -> str:
    if _is_compressed_tar(path):
        raise ValueError("Compressed WebDataset shards are not supported; expected an uncompressed .tar file")
    if any(character in path for character in "*?[") or path.lower().endswith(".tar"):
        return path
    return f"{path.rstrip('/')}/**/*.tar"


def _resolve_archive_paths(paths: str | list[str], io_config: IOConfig | None) -> list[str]:
    paths = [paths] if isinstance(paths, str) else paths
    if not paths:
        raise ValueError("Must specify at least one WebDataset path")

    archive_paths: list[str] = []
    seen_paths: set[str] = set()
    for path in paths:
        for file_info in io_glob(_glob_pattern(path), io_config=io_config):
            archive_path = file_info["path"]
            if file_info["type"] != "File" or not archive_path.lower().endswith(".tar"):
                continue
            if archive_path not in seen_paths:
                seen_paths.add(archive_path)
                archive_paths.append(archive_path)

    if not archive_paths:
        raise FileNotFoundError(f"No uncompressed WebDataset TAR shards found for: {paths}")
    return archive_paths


def _record_batch(samples: list[dict[str, object]], schema: Schema) -> RecordBatch:
    series = []
    for field in schema:
        values = [sample.get(field.name) for sample in samples]
        if _field_extension(field.name) in _JSON_EXTENSIONS and any(value is not None for value in values):
            inferred_dtype = Series.from_pylist(values, name=field.name).datatype()
            if inferred_dtype != field.dtype:
                raise ValueError(
                    f"WebDataset JSON field {field.name!r} does not match its inferred schema: "
                    f"expected {field.dtype}, received {inferred_dtype}"
                )
        series.append(Series.from_pylist(values, name=field.name, dtype=field.dtype))
    return RecordBatch._from_series(series, num_rows=len(samples))


def _iter_record_batches(
    archive_path: str,
    schema: Schema,
    known_fields: frozenset[str],
    io_config: IOConfig | None,
    batch_size: int,
) -> Iterator[RecordBatch]:
    selected_columns = set(schema.column_names())
    samples: list[dict[str, object]] = []
    for sample in _iter_archive_samples(
        archive_path,
        io_config,
        selected_columns=selected_columns,
        known_fields=known_fields,
    ):
        samples.append(sample)
        if len(samples) >= batch_size:
            yield _record_batch(samples, schema)
            samples.clear()

    if samples:
        yield _record_batch(samples, schema)


def _next_or_end(iterator: Iterator[RecordBatch]) -> RecordBatch | object:
    try:
        return next(iterator)
    except StopIteration:
        return _END_OF_ITERATOR


@PublicAPI
def read_webdataset(
    path: str | list[str],
    io_config: IOConfig | None = None,
    batch_size: int = 1000,
) -> DataFrame:
    """Read one or more WebDataset TAR shards.

    Consecutive TAR members with the same filename prefix are grouped into one row.
    The member suffix becomes the column name, following the WebDataset convention.
    Image, audio, video, and other binary members are represented by lazy
    :class:`daft.File` references into the TAR archive. JSON, text, and class/index
    sidecars are decoded eagerly.

    Args:
        path: An uncompressed TAR file, directory, glob, or list of paths. Directories
            are searched recursively for ``*.tar`` files.
        io_config: Configuration for local or remote storage.
        batch_size: Maximum number of samples yielded per record batch.

    Returns:
        A DataFrame containing ``__key__``, ``__url__``, and one column per member
        suffix discovered during schema inference.

    Note:
        Compressed TAR archives are not supported because member byte offsets cannot
        be used for lazy range-backed file references.
        Member suffixes and JSON shapes must be consistent across shards. The schema
        is inferred from the first five samples of the first shard, and later
        incompatibilities raise an error instead of discarding data.

    Examples:
        >>> df = daft.read_webdataset("/path/to/shards/*.tar")  # doctest: +SKIP
        >>> images = df.select("jpg")  # The image bytes remain lazy until opened or decoded.
    """
    if batch_size <= 0:
        raise ValueError(f"batch_size must be greater than zero, received {batch_size}")

    io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
    return WebDatasetSource(path, io_config=io_config, batch_size=batch_size).read()


class WebDatasetSource(DataSource):
    def __init__(
        self,
        paths: str | list[str],
        io_config: IOConfig | None,
        batch_size: int,
    ) -> None:
        self._archive_paths = _resolve_archive_paths(paths, io_config)
        self._io_config = io_config
        self._batch_size = batch_size
        self._schema = _infer_schema(self._archive_paths[0], io_config)

    @property
    def name(self) -> str:
        return "WebDatasetSource"

    @property
    def schema(self) -> Schema:
        return self._schema

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[WebDatasetSourceTask]:
        schema_columns = self._schema.column_names()
        if pushdowns.columns is None:
            projected_columns = schema_columns
        else:
            requested_columns = set(pushdowns.columns)
            projected_columns = [column for column in schema_columns if column in requested_columns]

        projected_schema = Schema.from_pydict({column: self._schema[column].dtype for column in projected_columns})
        task_batch_size = (
            min(self._batch_size, pushdowns.limit)
            if pushdowns.limit is not None and pushdowns.limit > 0
            else self._batch_size
        )
        for archive_path in self._archive_paths:
            yield WebDatasetSourceTask(
                _archive_path=archive_path,
                _schema=projected_schema,
                _known_fields=frozenset(schema_columns) - {"__key__", "__url__"},
                _io_config=self._io_config,
                _batch_size=task_batch_size,
            )


@dataclass
class WebDatasetSourceTask(DataSourceTask):
    _archive_path: str
    _schema: Schema
    _known_fields: frozenset[str]
    _io_config: IOConfig | None
    _batch_size: int

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        batches = _iter_record_batches(
            self._archive_path,
            self._schema,
            self._known_fields,
            self._io_config,
            self._batch_size,
        )
        while True:
            batch = await asyncio.to_thread(_next_or_end, batches)
            if batch is _END_OF_ITERATOR:
                break
            assert isinstance(batch, RecordBatch)
            yield batch
