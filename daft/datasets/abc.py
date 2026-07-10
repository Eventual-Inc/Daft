from __future__ import annotations

import json
import os
from collections.abc import Sequence
from typing import Any, Literal, cast

import daft
from daft.api_annotations import PublicAPI
from daft.dataframe import DataFrame
from daft.datatype import DataType
from daft.expressions import col, lit
from daft.file.mcap import McapFile
from daft.functions import mcap_file, regexp_extract, regexp_replace, when
from daft.io import IOConfig

_PUBLIC_HF_DATASET = "hf://datasets/XDOF/ABC-130k"

_DEFAULT_MESSAGE_TOPICS: tuple[str, ...] = (
    "/left-arm-state",
    "/right-arm-state",
    "/left-arm-action",
    "/right-arm-action",
    "/left-ee-state",
    "/right-ee-state",
    "/left-ee-action",
    "/right-ee-action",
)

_CAMERA_TOPICS: dict[str, tuple[str, ...]] = {
    "top": ("/top-camera", "/top-left-camera", "/top-right-camera"),
    "top_mono": ("/top-camera",),
    "top_left": ("/top-left-camera",),
    "top_right": ("/top-right-camera",),
    "left_wrist": ("/left-wrist-camera",),
    "right_wrist": ("/right-wrist-camera",),
}

_TOPIC_TO_CAMERA = {
    "/top-camera": "top_mono",
    "/top-left-camera": "top_left",
    "/top-right-camera": "top_right",
    "/left-wrist-camera": "left_wrist",
    "/right-wrist-camera": "right_wrist",
}

_METADATA_DTYPE = DataType.struct(
    {
        "session_id": DataType.string(),
        "operator_id": DataType.string(),
        "task_name": DataType.string(),
        "duration_seconds": DataType.float64(),
        "message_count": DataType.uint64(),
        "message_start_time": DataType.uint64(),
        "message_end_time": DataType.uint64(),
        "chunk_count": DataType.uint64(),
        "topics": DataType.list(DataType.string()),
        "video_topics": DataType.list(DataType.string()),
        "indexed": DataType.bool(),
        "episode_metadata_json": DataType.string(),
    }
)

_ANNOTATION_DTYPE = DataType.struct(
    {
        "timestamp_ns": DataType.int64(),
        "label": DataType.string(),
    }
)


def _first_value(values: dict[str, str], *keys: str) -> str | None:
    for key in keys:
        value = values.get(key)
        if value not in (None, ""):
            return value
    return None


@daft.func(return_dtype=_METADATA_DTYPE, use_process=False, unnest=True)
def _read_abc_metadata(file: McapFile) -> dict[str, object]:
    info = file._read_metadata(include_metadata_records=True)
    statistics = info["statistics"]

    records = info["metadata"]
    record = next((record for record in records if record["name"] == "episode-metadata"), None)
    if record is None:
        record = next((record for record in records if record["name"] == "session-metadata"), None)
    values = {} if record is None else record["metadata"]

    duration: float | None = None
    raw_duration = _first_value(
        values,
        "duration",
        "duration_seconds",
        "duration-seconds",
        "episode-duration",
    )
    if raw_duration is not None:
        try:
            duration = float(raw_duration)
        except ValueError:
            pass
    if duration is None and statistics is not None:
        duration = (statistics["message_end_time"] - statistics["message_start_time"]) / 1_000_000_000

    topics = list(dict.fromkeys(channel["topic"] for channel in info["channels"]))
    video_topics = list(
        dict.fromkeys(
            channel["topic"] for channel in info["channels"] if channel["schema_name"] == "foxglove.CompressedVideo"
        )
    )

    return {
        "session_id": _first_value(values, "session_id", "session-id", "session_uuid", "session-uuid"),
        "operator_id": _first_value(
            values,
            "operator_id",
            "operator-id",
            "operator_uuid",
            "operator-uuid",
        ),
        "task_name": _first_value(values, "task_name", "task-name", "instruction"),
        "duration_seconds": duration,
        "message_count": None if statistics is None else statistics["message_count"],
        "message_start_time": None if statistics is None else statistics["message_start_time"],
        "message_end_time": None if statistics is None else statistics["message_end_time"],
        "chunk_count": None if statistics is None else statistics["chunk_count"],
        "topics": topics,
        "video_topics": video_topics,
        "indexed": info["indexed"],
        "episode_metadata_json": (
            None
            if record is None
            else json.dumps(
                {"name": record["name"], "metadata": values},
                separators=(",", ":"),
                sort_keys=True,
            )
        ),
    }


def _read_varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while offset < len(data) and shift < 70:
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value, offset
        shift += 7
    raise ValueError("Malformed ABC annotation protobuf varint")


def _read_length_delimited(data: bytes, offset: int) -> tuple[bytes, int]:
    length, offset = _read_varint(data, offset)
    end = offset + length
    if end > len(data):
        raise ValueError("Truncated ABC annotation protobuf field")
    return data[offset:end], end


def _skip_protobuf_field(data: bytes, offset: int, wire_type: int) -> int:
    if wire_type == 0:
        _, offset = _read_varint(data, offset)
        return offset
    if wire_type == 1:
        end = offset + 8
    elif wire_type == 2:
        _, end = _read_length_delimited(data, offset)
        return end
    elif wire_type == 5:
        end = offset + 4
    else:
        raise ValueError(f"Unsupported protobuf wire type {wire_type} in ABC annotation")
    if end > len(data):
        raise ValueError("Truncated ABC annotation protobuf field")
    return end


def _parse_timestamp(data: bytes) -> int:
    seconds = 0
    nanos = 0
    offset = 0
    while offset < len(data):
        tag, offset = _read_varint(data, offset)
        field_number, wire_type = tag >> 3, tag & 0x07
        if field_number == 1 and wire_type == 0:
            seconds, offset = _read_varint(data, offset)
            if seconds >= 1 << 63:
                seconds -= 1 << 64
        elif field_number == 2 and wire_type == 0:
            nanos, offset = _read_varint(data, offset)
            if nanos >= 1 << 31:
                nanos -= 1 << 32
        else:
            offset = _skip_protobuf_field(data, offset, wire_type)
    if not 0 <= nanos < 1_000_000_000:
        raise ValueError(f"Invalid ABC annotation timestamp nanos: {nanos}")
    return seconds * 1_000_000_000 + nanos


@daft.func(return_dtype=_ANNOTATION_DTYPE, use_process=False, unnest=True)
def _parse_abc_annotation(data: bytes) -> dict[str, object]:
    timestamp_ns: int | None = None
    label: str | None = None
    offset = 0
    try:
        while offset < len(data):
            tag, offset = _read_varint(data, offset)
            field_number, wire_type = tag >> 3, tag & 0x07
            if tag == 0:
                raise ValueError("Invalid zero protobuf tag in ABC annotation")
            if field_number in {1, 2} and wire_type != 2:
                raise ValueError(f"Invalid wire type for ABC annotation field {field_number}")
            if field_number == 1:
                value, offset = _read_length_delimited(data, offset)
                timestamp_ns = _parse_timestamp(value)
            elif field_number == 2:
                value, offset = _read_length_delimited(data, offset)
                label = value.decode("utf-8")
            else:
                offset = _skip_protobuf_field(data, offset, wire_type)
    except (UnicodeDecodeError, ValueError):
        return {"timestamp_ns": None, "label": None}
    return {"timestamp_ns": timestamp_ns, "label": label}


def _normalize_names(value: str | Sequence[str] | None, *, name: str) -> tuple[str, ...] | None:
    if value is None:
        return None
    values = (value,) if isinstance(value, str) else tuple(value)
    if not values:
        raise ValueError(f"{name} must contain at least one value")
    if any(not isinstance(item, str) or not item for item in values):
        raise ValueError(f"{name} values must be non-empty strings")
    return tuple(dict.fromkeys(values))


def _resolve_hf_io_config(io_config: IOConfig | None, paths: Sequence[str]) -> IOConfig | None:
    if not any(path.startswith("hf://") for path in paths):
        return io_config
    if io_config is not None and (io_config.hf.anonymous or io_config.hf.token is not None):
        return io_config
    token = os.environ.get("HF_TOKEN")
    if token is None:
        try:
            from huggingface_hub import get_token

            token = get_token()
        except ImportError:
            pass
    if token is None:
        return io_config
    from daft.io import HuggingFaceConfig, IOConfig

    if io_config is not None:
        return io_config.replace(hf=io_config.hf.replace(token=token))
    return IOConfig(hf=HuggingFaceConfig(token=token, use_xet=True))


def _empty_object_listing() -> DataFrame:
    return daft.from_pydict({"path": [""], "size": [0]}).where(lit(False))


def _hf_catalog_objects(
    path: str,
    *,
    split: Literal["train", "val"] | None,
    tasks: tuple[str, ...] | None,
    include_annotations: bool,
    io_config: IOConfig | None,
) -> DataFrame:
    """List ABC objects through the Hub's recursive tree endpoint.

    The generic HF glob bridge walks each episode directory independently,
    which is both slow and prone to Hub rate limits on ABC's deep layout. The
    official client performs paginated recursive tree reads server-side.
    """
    try:
        from huggingface_hub import HfApi
        from huggingface_hub.hf_api import RepoFile
        from huggingface_hub.utils import EntryNotFoundError
    except ImportError as error:
        raise ImportError(
            "The 'daft[huggingface]' extra is required to discover ABC-130k on Hugging Face. "
            "Please install it with: pip install 'daft[huggingface]'"
        ) from error

    prefix = "hf://datasets/"
    root = path.rstrip("/")
    io_config = _resolve_hf_io_config(io_config, (root,))
    remainder = root.removeprefix(prefix)
    parts = remainder.split("/")
    if not root.startswith(prefix) or len(parts) != 2:
        raise ValueError("ABC Hugging Face path must be a dataset root such as hf://datasets/XDOF/ABC-130k")
    namespace, name_revision = parts
    name, _, revision = name_revision.partition("@")
    repo_id = f"{namespace}/{name}"

    token: str | bool | None = None
    if io_config is not None:
        token = False if io_config.hf.anonymous else io_config.hf.token
    api = HfApi(token=token, library_name="daft")

    search_root_groups: list[tuple[str, ...]]
    if tasks is None:
        search_root_groups = [(f"data/{split}" if split is not None else "data",)]
    else:
        splits = (split,) if split is not None else ("train", "val")
        search_root_groups = [
            (f"data/{selected_split}/{task}", f"data/{selected_split}/task={task}")
            for selected_split in splits
            for task in tasks
        ]

    paths: list[str] = []
    sizes: list[int | None] = []
    wanted_names = {"episode.mcap", "annotation.mcap"} if include_annotations else {"episode.mcap"}
    for search_roots in search_root_groups:
        for search_root in search_roots:
            matched = False
            try:
                entries = api.list_repo_tree(
                    repo_id,
                    path_in_repo=search_root,
                    recursive=True,
                    revision=revision or None,
                    repo_type="dataset",
                )
                for entry in entries:
                    if isinstance(entry, RepoFile) and entry.path.rsplit("/", 1)[-1] in wanted_names:
                        paths.append(f"{root}/{entry.path}")
                        sizes.append(entry.size)
                        matched = True
            except EntryNotFoundError:
                pass
            if matched:
                break

    if not paths:
        return _empty_object_listing()
    return daft.from_pydict({"path": paths, "size": sizes})


def _validate_episode_columns(episodes: DataFrame, *columns: str) -> None:
    available = set(episodes.schema().column_names())
    missing = [column for column in columns if column not in available]
    if missing:
        raise ValueError(f"Expected an ABC episode DataFrame with columns: {missing}")


def _collect_paths(episodes: DataFrame, columns: Sequence[str]) -> list[str]:
    _validate_episode_columns(episodes, *columns)
    selected = episodes.select(*columns).to_pydict()
    paths: list[str] = []
    for column in columns:
        paths.extend(value for value in selected[column] if value is not None)
    return list(dict.fromkeys(paths))


def _with_episode_identity(messages: DataFrame) -> DataFrame:
    source = col("source_path")
    return messages.with_columns(
        {
            "split": regexp_extract(source, r"/data/(train|val)/", 1),
            "task_slug": regexp_extract(source, r"/data/(?:train|val)/(?:task=)?([^/]+)/episode_", 1),
            "episode_id": regexp_extract(source, r"/episode_([^/]+)/", 1),
            "episode_dir": regexp_replace(source, r"/[^/]+\.mcap$", ""),
            "file_kind": regexp_extract(source, r"/(episode|annotation)\.mcap$", 1),
        }
    )


@PublicAPI
def raw(
    path: str = _PUBLIC_HF_DATASET,
    *,
    split: Literal["train", "val"] | None = None,
    tasks: str | Sequence[str] | None = None,
    include_annotations: bool = True,
    io_config: IOConfig | None = None,
) -> DataFrame:
    r"""Discover ABC-130k episodes as an episode-level catalog.

    This performs object listing and path parsing only. The Hugging Face source
    eagerly consumes the official client's paginated recursive-tree API before
    returning; other stores use Daft's lazy glob source. Subsequent DataFrame
    transforms remain lazy. Discovery deliberately does not open the large MCAP
    objects or sniff their summaries; call :func:`metadata` after filtering or
    limiting the returned DataFrame.

    Args:
        path: ABC-130k dataset root. Defaults to the gated public Hugging Face dataset.
        split: Optional ``"train"`` or ``"val"`` split restriction.
        tasks: Optional task slug or sequence of task slugs. Task selection is
            included in the listing prefix to avoid traversing unrelated tasks.
        include_annotations: Include optional ``annotation.mcap`` objects in the
            catalog. Disabling this retains only ``episode.mcap`` objects.
        io_config: IO configuration for remote storage and credentials.

    Returns:
        One row per episode with path-derived identity, object sizes, and lazy
        ``episode_mcap`` and nullable ``annotation_mcap`` file references.

    Examples:
        >>> from daft.datasets import abc
        >>> episodes = abc.raw(split="train", tasks="clip_the_socks_to_the_hanger")  # doctest: +SKIP
        >>> sample = episodes.where(episodes["annotated"]).limit(4)  # doctest: +SKIP
    """
    if split not in (None, "train", "val"):
        raise ValueError("split must be one of: 'train', 'val', or None")
    selected_tasks = _normalize_names(tasks, name="tasks")
    if selected_tasks is not None and any("/" in task or "*" in task for task in selected_tasks):
        raise ValueError("tasks must contain exact task slugs, without '/' or glob wildcards")
    if selected_tasks is not None:
        selected_tasks = tuple(dict.fromkeys(task.removeprefix("task=") for task in selected_tasks))

    root = path.rstrip("/")
    io_config = _resolve_hf_io_config(io_config, (root,))
    split_pattern = split or "*"
    filename = "*.mcap" if include_annotations else "episode.mcap"
    if selected_tasks is None:
        patterns = [f"{root}/data/{split_pattern}/**/{filename}"]
    else:
        task_directories = [directory for task in selected_tasks for directory in (task, f"task={task}")]
        patterns = [f"{root}/data/{split_pattern}/{directory}/**/{filename}" for directory in task_directories]

    if root.startswith("hf://datasets/"):
        objects = _hf_catalog_objects(
            root,
            split=split,
            tasks=selected_tasks,
            include_annotations=include_annotations,
            io_config=io_config,
        )
    else:
        listings = [daft.from_glob_path(pattern, io_config=io_config) for pattern in patterns]
        objects = listings[0] if len(listings) == 1 else daft.concat(listings)
    objects = daft.concat([objects.select("path", "size"), _empty_object_listing()])
    objects = (
        objects.select(
            "path",
            "size",
            regexp_extract(col("path"), r"/(episode|annotation)\.mcap$", 1).alias("__file_kind"),
            regexp_extract(col("path"), r"/data/(train|val)/", 1).alias("split"),
            regexp_extract(col("path"), r"/data/(?:train|val)/(?:task=)?([^/]+)/episode_", 1).alias("task_slug"),
            regexp_extract(col("path"), r"/episode_([^/]+)/", 1).alias("episode_id"),
            regexp_replace(col("path"), r"/[^/]+\.mcap$", "").alias("episode_dir"),
        )
        .where(col("__file_kind").not_null())
        .groupby("split", "task_slug", "episode_id", "episode_dir")
        .agg(
            when(col("__file_kind") == "episode", col("path"))
            .otherwise(lit(None))
            .any_value(ignore_nulls=True)
            .alias("episode_path"),
            when(col("__file_kind") == "episode", col("size"))
            .otherwise(lit(None))
            .any_value(ignore_nulls=True)
            .alias("episode_size"),
            when(col("__file_kind") == "annotation", col("path"))
            .otherwise(lit(None))
            .any_value(ignore_nulls=True)
            .alias("annotation_path"),
            when(col("__file_kind") == "annotation", col("size"))
            .otherwise(lit(None))
            .any_value(ignore_nulls=True)
            .alias("annotation_size"),
        )
        .where(col("episode_path").not_null())
        .with_columns(
            {
                "annotated": col("annotation_path").not_null(),
                "episode_mcap": mcap_file(col("episode_path"), io_config=io_config),
                "annotation_mcap": when(
                    col("annotation_path").not_null(),
                    mcap_file(col("annotation_path"), io_config=io_config),
                ).otherwise(lit(None)),
            }
        )
    )
    return objects.select(
        "split",
        "task_slug",
        "episode_id",
        "episode_dir",
        "episode_path",
        "episode_size",
        "annotation_path",
        "annotation_size",
        "annotated",
        "episode_mcap",
        "annotation_mcap",
    )


@PublicAPI
def metadata(episodes: DataFrame) -> DataFrame:
    r"""Range-read MCAP summaries for an already bounded ABC episode catalog.

    Filter and limit the catalog from :func:`raw` before calling this function.
    Each selected MCAP performs small header/footer/summary range reads; message
    payloads are not downloaded.

    Args:
        episodes: Episode-level DataFrame from :func:`raw`, containing an
            ``episode_mcap`` column.

    Returns:
        The input episode columns plus normalized session/task fields, indexed
        message statistics, topic catalogs, and ``episode_metadata_json``.

    Examples:
        >>> from daft.datasets import abc
        >>> sample = abc.raw(split="train", tasks="clip_the_socks_to_the_hanger").limit(4)  # doctest: +SKIP
        >>> abc.metadata(sample).select("episode_id", "message_count", "topics").show()  # doctest: +SKIP
    """
    _validate_episode_columns(episodes, "episode_mcap")
    columns = episodes.schema().column_names()
    return episodes.where(col("episode_mcap").not_null()).select(
        *columns,
        _read_abc_metadata(col("episode_mcap")),
    )


@PublicAPI
def messages(
    episodes: DataFrame,
    *,
    topics: str | Sequence[str] | None = _DEFAULT_MESSAGE_TOPICS,
    start_time: int | None = None,
    end_time: int | None = None,
    files: Literal["episode", "annotation", "both"] = "episode",
    batch_size: int = 1000,
    io_config: IOConfig | None = None,
) -> DataFrame:
    r"""Read independently timestamped ABC MCAP messages from a bounded episode catalog.

    The selected catalog paths are collected eagerly, then passed as exact paths
    to the native MCAP source. Topic and time predicates remain scanner
    pushdowns. Apply episode filters and a limit before this call.

    Args:
        episodes: Bounded episode-level DataFrame from :func:`raw`.
        topics: Topic or topics to read. Defaults to the eight arm/gripper
            state and action streams. Pass ``None`` to read every topic.
        start_time: Inclusive MCAP ``log_time`` bound, normally Unix nanoseconds.
        end_time: Exclusive MCAP ``log_time`` bound, normally Unix nanoseconds.
        files: Read ``episode.mcap``, ``annotation.mcap``, or both.
        batch_size: Maximum encoded messages per source batch.
        io_config: IO configuration for reading selected paths. When omitted,
            Hugging Face credentials are resolved from ``HF_TOKEN`` or the Hub login.

    Returns:
        A long-form message DataFrame with binary ``data``, native MCAP fields,
        source provenance, and path-derived episode identity. Streams are not
        implicitly aligned or resampled.

    Examples:
        >>> from daft.datasets import abc
        >>> one = abc.raw(split="train", tasks="clip_the_socks_to_the_hanger").limit(1)  # doctest: +SKIP
        >>> states = abc.messages(one, topics="/left-arm-state")  # doctest: +SKIP
    """
    if files not in ("episode", "annotation", "both"):
        raise ValueError("files must be one of: 'episode', 'annotation', or 'both'")
    selected_topics = _normalize_names(topics, name="topics")
    path_columns = {
        "episode": ("episode_path",),
        "annotation": ("annotation_path",),
        "both": ("episode_path", "annotation_path"),
    }[files]
    paths = _collect_paths(episodes, path_columns)
    io_config = _resolve_hf_io_config(io_config, paths)
    dataframe = daft.read_mcap(
        paths,
        io_config=io_config,
        start_time=start_time,
        end_time=end_time,
        topics=None if selected_topics is None else list(selected_topics),
        batch_size=batch_size,
    )
    return _with_episode_identity(dataframe)


@PublicAPI  # type: ignore[no-redef]
def annotations(
    episodes: DataFrame,
    *,
    start_time: int | None = None,
    end_time: int | None = None,
    batch_size: int = 1000,
    io_config: IOConfig | None = None,
) -> DataFrame:
    r"""Read typed ``/subtask-annotation`` events from selected ABC episodes.

    Args:
        episodes: Bounded episode-level DataFrame from :func:`raw`.
        start_time: Inclusive annotation ``log_time`` bound.
        end_time: Exclusive annotation ``log_time`` bound.
        batch_size: Maximum encoded messages per source batch.
        io_config: IO configuration for reading annotation paths.

    Returns:
        One event row per free-form subtask label, including ``timestamp_ns``,
        ``label``, source provenance, and episode identity. Episodes without an
        annotation MCAP contribute no rows.
    """
    dataframe = messages(
        episodes,
        topics=("/subtask-annotation",),
        start_time=start_time,
        end_time=end_time,
        files="annotation",
        batch_size=batch_size,
        io_config=io_config,
    )
    return dataframe.select(
        "split",
        "task_slug",
        "episode_id",
        "episode_dir",
        "file_kind",
        "source_path",
        "topic",
        "log_time",
        "publish_time",
        "sequence",
        _parse_abc_annotation(col("data")),
    )


@PublicAPI
def camera_frames(
    episodes: DataFrame,
    cameras: str | Sequence[str] = ("top", "left_wrist", "right_wrist"),
    *,
    start_time: int | None = None,
    end_time: int | None = None,
    width: int | None = None,
    height: int | None = None,
    batch_size: int = 1000,
    io_config: IOConfig | None = None,
) -> DataFrame:
    r"""Decode ABC camera topics into frame-level image rows.

    ``top`` expands to mono, left, and right top-camera topics and keeps the
    concrete topic/camera name in every output row. Codec selection comes from
    each Foxglove ``CompressedVideo.format`` payload, including H.264 and H.265.

    Args:
        episodes: Bounded episode-level DataFrame from :func:`raw`.
        cameras: Camera alias or aliases. Supported values are ``top``,
            ``top_mono``, ``top_left``, ``top_right``, ``left_wrist``, and
            ``right_wrist``.
        start_time: Inclusive MCAP ``log_time`` bound.
        end_time: Exclusive MCAP ``log_time`` bound.
        width: Optional output image width; must be supplied with ``height``.
        height: Optional output image height; must be supplied with ``width``.
        batch_size: Maximum encoded messages per source batch; decoded output
            is additionally bounded by a byte budget.
        io_config: IO configuration for reading episode paths.

    Returns:
        One row per decoded frame with image ``data``, codec/frame metadata,
        concrete ``camera`` and ``topic``, source provenance, and episode identity.
    """
    from daft.dependencies import av

    if not cast("Any", av).module_available():
        raise ImportError(
            "The 'daft[video]' extra is required to decode ABC camera frames. "
            "Please install it with: pip install 'daft[video]'"
        )
    selected_cameras = _normalize_names(cameras, name="cameras")
    assert selected_cameras is not None
    unknown = [camera for camera in selected_cameras if camera not in _CAMERA_TOPICS]
    if unknown:
        expected = ", ".join(_CAMERA_TOPICS)
        raise ValueError(f"Unknown camera(s): {unknown}. Expected one or more of: {expected}.")
    topics = list(dict.fromkeys(topic for camera in selected_cameras for topic in _CAMERA_TOPICS[camera]))
    paths = _collect_paths(episodes, ("episode_path",))
    io_config = _resolve_hf_io_config(io_config, paths)
    dataframe = daft.read_mcap(
        paths,
        io_config=io_config,
        start_time=start_time,
        end_time=end_time,
        topics=topics,
        batch_size=batch_size,
        decode_video=True,
        image_height=height,
        image_width=width,
    )
    camera = when(col("topic") == "/top-camera", lit(_TOPIC_TO_CAMERA["/top-camera"]))
    for topic, name in tuple(_TOPIC_TO_CAMERA.items())[1:]:
        camera = camera.when(col("topic") == topic, lit(name))
    return _with_episode_identity(dataframe).with_column("camera", camera.otherwise(lit(None)))


__all__ = [
    "annotations",
    "camera_frames",
    "messages",
    "metadata",
    "raw",
]
