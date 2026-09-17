from __future__ import annotations

import json
import math
import os
import re
import struct
import uuid
import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from daft.datatype import DataType
from daft.dependencies import pa, pafs, pc, pq
from daft.filesystem import _resolve_paths_and_filesystem
from daft.io.sink import DataSink, WriteResult
from daft.recordbatch import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Iterator

    from daft.daft import IOConfig


_TASK_COLUMN = "__daft_lerobot_task"
_DATA_PATH = "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet"
_EPISODES_PATH = "meta/episodes/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet"
_CANONICAL_FEATURES: dict[str, dict[str, Any]] = {
    "timestamp": {"dtype": "float32", "shape": [1], "names": None},
    "frame_index": {"dtype": "int64", "shape": [1], "names": None},
    "episode_index": {"dtype": "int64", "shape": [1], "names": None},
    "index": {"dtype": "int64", "shape": [1], "names": None},
    "task_index": {"dtype": "int64", "shape": [1], "names": None},
}
_NUMPY_DTYPES: dict[str, pa.DataType] = {
    "bool": pa.bool_(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float16": pa.float16(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "string": pa.string(),
}
_QUANTILES = (0.01, 0.10, 0.50, 0.90, 0.99)
_ARROW_CONTAINER_KEY = "__daft_arrow_container"
_ARROW_TENSOR_TYPE_KEY = "__daft_arrow_tensor_type"
_SUPPORTED_CODEBASE_VERSION = re.compile(r"^v(?:2\.(?:0|1)|3(?:\.\d+)?)$")


@dataclass(frozen=True)
class LeRobotStagedShard:
    path: str
    min_episode_index: int
    min_frame_index: int


@dataclass
class _PreparedEpisode:
    table: pa.Table
    metadata: dict[str, Any]
    stats: dict[str, dict[str, list[int | float]]]


def _primitive_arrow_type(dtype: str) -> pa.DataType:
    try:
        return _NUMPY_DTYPES[dtype]
    except KeyError:
        raise ValueError(f"Unsupported LeRobot dtype {dtype!r}; supported dtypes are {sorted(_NUMPY_DTYPES)}") from None


def feature_arrow_type(name: str, feature: dict[str, Any]) -> pa.DataType:
    """Return the Arrow storage type for a LeRobot feature descriptor."""
    dtype = str(feature.get("dtype"))
    if dtype in {"image", "video", "language"}:
        raise NotImplementedError(
            f"write_lerobot does not yet support {dtype!r} feature {name!r}; "
            "encode visual columns before writing or omit them"
        )
    primitive = _primitive_arrow_type(dtype)
    shape = feature.get("shape")
    if not isinstance(shape, (list, tuple)) or not shape or not all(isinstance(v, int) and v > 0 for v in shape):
        raise ValueError(f"LeRobot feature {name!r} must have a non-empty positive integer shape, got {shape!r}")
    container = feature.get(_ARROW_CONTAINER_KEY)
    if container == "tensor":
        return feature[_ARROW_TENSOR_TYPE_KEY]
    preserve_container = container == "fixed_size_list"
    if dtype == "string" and not preserve_container:
        if list(shape) != [1]:
            raise ValueError(f"String feature {name!r} must have shape [1], got {shape!r}")
        return primitive
    if list(shape) == [1] and not preserve_container:
        return primitive
    result = primitive
    for size in reversed(shape):
        result = pa.list_(result, int(size))
    return result


def _infer_primitive_and_shape(arrow_type: pa.DataType) -> tuple[pa.DataType, list[int]]:
    if isinstance(arrow_type, pa.FixedShapeTensorType):
        return arrow_type.value_type, list(arrow_type.shape)

    shape: list[int] = []
    current = arrow_type
    while pa.types.is_fixed_size_list(current):
        shape.append(current.list_size)
        current = current.value_type
    if pa.types.is_list(current) or pa.types.is_large_list(current):
        raise ValueError("variable-length list columns cannot be represented as fixed-shape LeRobot features")
    return current, shape or [1]


def _dtype_name(arrow_type: pa.DataType) -> str:
    if pa.types.is_large_string(arrow_type):
        return "string"
    for name, candidate in _NUMPY_DTYPES.items():
        if arrow_type == candidate:
            return name
    raise ValueError(f"Arrow type {arrow_type} cannot be represented as a LeRobot feature")


def normalize_lerobot_features(
    arrow_schema: pa.Schema,
    feature_names: list[str],
    features: dict[str, dict[str, Any]] | None,
    fps: int,
) -> dict[str, dict[str, Any]]:
    """Infer and validate LeRobot descriptors for the user feature columns."""
    if not isinstance(fps, int) or isinstance(fps, bool) or fps <= 0:
        raise ValueError(f"fps must be a positive integer, got {fps!r}")

    supplied = features or {}
    unknown = sorted(set(supplied) - set(feature_names))
    if unknown:
        raise ValueError(f"features contains descriptors for columns that are not written: {unknown}")

    normalized: dict[str, dict[str, Any]] = {}
    for name in feature_names:
        if "/" in name:
            raise ValueError(f"LeRobot feature names cannot contain '/': {name!r}")
        field = arrow_schema.field(name)
        if name in supplied:
            descriptor = dict(supplied[name])
            descriptor["shape"] = list(descriptor.get("shape", []))
            descriptor.setdefault("names", None)
        else:
            primitive, shape = _infer_primitive_and_shape(field.type)
            descriptor = {"dtype": _dtype_name(primitive), "shape": shape, "names": None}

        if isinstance(field.type, pa.FixedShapeTensorType):
            if field.type.permutation is not None:
                raise ValueError(f"Permuted FixedShapeTensor column {name!r} is not supported")
            if descriptor["shape"] != list(field.type.shape):
                raise ValueError(
                    f"Column {name!r} has FixedShapeTensor shape {list(field.type.shape)!r}, "
                    f"which does not match declared LeRobot shape {descriptor['shape']!r}"
                )
            descriptor[_ARROW_CONTAINER_KEY] = "tensor"
            descriptor[_ARROW_TENSOR_TYPE_KEY] = field.type
        elif pa.types.is_fixed_size_list(field.type) and descriptor["dtype"] != "string":
            # LeRobot uses shape [1] for both scalar values and one-element
            # vectors. Preserve Arrow's container storage so the latter is not
            # incorrectly cast to a scalar. This private hint is removed before
            # descriptors are persisted to info.json.
            descriptor[_ARROW_CONTAINER_KEY] = "fixed_size_list"

        target_type = feature_arrow_type(name, descriptor)
        if not (pa.types.is_string(target_type) and pa.types.is_large_string(field.type)):
            try:
                pc.cast(pa.array([], type=field.type), target_type)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError, TypeError) as exc:
                raise ValueError(
                    f"Column {name!r} with Arrow type {field.type} cannot be cast to declared "
                    f"LeRobot feature {descriptor!r}"
                ) from exc
        normalized[name] = descriptor
    return normalized


def _json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _flatten_numbers(value: Any) -> list[float]:
    if isinstance(value, (list, tuple)):
        return [number for item in value for number in _flatten_numbers(item)]
    return [float(value)]


def _reshape_numbers(values: list[float], shape: tuple[int, ...]) -> Any:
    if len(shape) == 1:
        return values
    stride = math.prod(shape[1:])
    return [_reshape_numbers(values[offset : offset + stride], shape[1:]) for offset in range(0, len(values), stride)]


def _reshape_like(values: list[float], template: Any) -> Any:
    iterator = iter(values)

    def rebuild(item: Any) -> Any:
        if isinstance(item, list):
            return [rebuild(child) for child in item]
        return next(iterator)

    return rebuild(template)


def _numeric_coordinates(values: pa.Array) -> Iterator[pa.Array]:
    if pa.types.is_fixed_size_list(values.type):
        for index in range(values.type.list_size):
            yield from _numeric_coordinates(pc.list_element(values, index))
    else:
        # LeRobot statistics use float64 even for integer features. Match that
        # lossy conversion for int64/uint64 values outside float64's exact range
        # while preserving the original integers in the data shards.
        yield pc.cast(values, pa.float64(), safe=False)


def _tensor_to_fixed_size_lists(values: pa.ExtensionArray, shape: list[int], primitive: pa.DataType) -> pa.Array:
    """Convert Arrow's flat FixedShapeTensor storage to LeRobot's nested list storage."""
    storage = values.storage
    result = pc.cast(storage.values, primitive)
    if shape == [1]:
        return pc.if_else(storage.is_null(), pa.scalar(None, type=primitive), result)
    for depth, size in enumerate(reversed(shape)):
        mask = storage.is_null() if depth == len(shape) - 1 else None
        result = pa.FixedSizeListArray.from_arrays(result, size, mask=mask)
    return result


def _weighted_average(left: float, right: float, left_count: int, right_count: int) -> float:
    total = left_count + right_count
    return left * (left_count / total) + right * (right_count / total)


def _combined_std(
    left_std: float,
    right_std: float,
    left_mean: float,
    right_mean: float,
    mean: float,
    left_count: int,
    right_count: int,
) -> float:
    left_delta = left_mean - mean
    right_delta = right_mean - mean
    scale = max(abs(left_std), abs(right_std), abs(left_delta), abs(right_delta))
    if scale == 0:
        return 0.0
    if math.isinf(scale):
        return math.inf
    total = left_count + right_count
    variance = (
        ((left_std / scale) ** 2 + (left_delta / scale) ** 2) * left_count
        + ((right_std / scale) ** 2 + (right_delta / scale) ** 2) * right_count
    ) / total
    return scale * math.sqrt(variance)


def _feature_stats(table: pa.Table, features: dict[str, dict[str, Any]]) -> dict[str, dict[str, list[int | float]]]:
    stats: dict[str, dict[str, list[int | float]]] = {}
    for name, feature in features.items():
        if feature["dtype"] in {"string", "language", "image", "video"}:
            continue
        column = table[name].combine_chunks()
        if column.null_count:
            raise ValueError(f"LeRobot feature {name!r} contains null values")
        shape = tuple(feature["shape"])
        coordinate_count = math.prod(shape)
        flat_stats: dict[str, list[float]] = {
            "min": [],
            "max": [],
            "mean": [],
            "std": [],
            **{f"q{int(quantile * 100):02d}": [] for quantile in _QUANTILES},
        }
        observed_coordinates = 0
        for coordinate in _numeric_coordinates(column):
            observed_coordinates += 1
            if coordinate.null_count:
                raise ValueError(f"LeRobot feature {name!r} contains null values")
            if not pc.all(pc.is_finite(coordinate)).as_py():
                raise ValueError(f"LeRobot feature {name!r} contains non-finite values")
            flat_stats["min"].append(float(pc.min(coordinate).as_py()))
            flat_stats["max"].append(float(pc.max(coordinate).as_py()))
            flat_stats["mean"].append(float(pc.mean(coordinate).as_py()))
            flat_stats["std"].append(float(pc.stddev(coordinate, ddof=0).as_py()))
            for quantile in _QUANTILES:
                key = f"q{int(quantile * 100):02d}"
                flat_stats[key].append(float(pc.quantile(coordinate, q=quantile, interpolation="linear")[0].as_py()))
        if observed_coordinates != coordinate_count:
            raise ValueError(f"LeRobot feature {name!r} does not match declared shape {list(shape)!r}")
        feature_stats: dict[str, Any] = {key: _reshape_numbers(values, shape) for key, values in flat_stats.items()}
        feature_stats["count"] = [len(table)]
        stats[name] = _json_value(feature_stats)
    return stats


def _aggregate_stats(
    current: dict[str, dict[str, list[int | float]]] | None,
    incoming: dict[str, dict[str, list[int | float]]],
) -> dict[str, dict[str, list[int | float]]]:
    if current is None:
        return incoming
    result: dict[str, dict[str, list[int | float]]] = {}
    for name in current.keys() | incoming.keys():
        if name not in current:
            result[name] = incoming[name]
            continue
        if name not in incoming:
            result[name] = current[name]
            continue
        left, right = current[name], incoming[name]
        left_count = int(left["count"][0])
        right_count = int(right["count"][0])
        total = left_count + right_count
        left_mean = _flatten_numbers(left["mean"])
        right_mean = _flatten_numbers(right["mean"])
        mean = [
            _weighted_average(left_value, right_value, left_count, right_count)
            for left_value, right_value in zip(left_mean, right_mean, strict=True)
        ]
        left_std = _flatten_numbers(left["std"])
        right_std = _flatten_numbers(right["std"])
        std = [
            _combined_std(
                left_std_value,
                right_std_value,
                left_mean_value,
                right_mean_value,
                mean_value,
                left_count,
                right_count,
            )
            for left_std_value, right_std_value, left_mean_value, right_mean_value, mean_value in zip(
                left_std, right_std, left_mean, right_mean, mean, strict=True
            )
        ]
        template = left["mean"]
        merged: dict[str, Any] = {
            "min": _reshape_like(
                [
                    min(left_value, right_value)
                    for left_value, right_value in zip(
                        _flatten_numbers(left["min"]), _flatten_numbers(right["min"]), strict=True
                    )
                ],
                template,
            ),
            "max": _reshape_like(
                [
                    max(left_value, right_value)
                    for left_value, right_value in zip(
                        _flatten_numbers(left["max"]), _flatten_numbers(right["max"]), strict=True
                    )
                ],
                template,
            ),
            "mean": _reshape_like(mean, template),
            "std": _reshape_like(std, template),
            "count": [total],
        }
        for quantile in _QUANTILES:
            key = f"q{int(quantile * 100):02d}"
            # Match LeRobot's aggregate_feature_stats semantics. Exact global
            # quantiles cannot be recovered from per-episode summaries, so
            # lower quantiles use the minimum estimate and upper quantiles use
            # the maximum estimate as conservative bounds.
            operation = min if quantile <= 0.5 else max
            merged[key] = _reshape_like(
                [
                    operation(left_value, right_value)
                    for left_value, right_value in zip(
                        _flatten_numbers(left[key]), _flatten_numbers(right[key]), strict=True
                    )
                ],
                template,
            )
        result[name] = _json_value(merged)
    return result


def _episode_ranges(table: pa.Table) -> list[tuple[int, int]]:
    episode_indices = table["episode_index"].combine_chunks().to_pylist()
    starts = [0]
    starts.extend(
        index for index in range(1, len(episode_indices)) if episode_indices[index] != episode_indices[index - 1]
    )
    stops = [*starts[1:], len(table)]
    return list(zip(starts, stops, strict=True))


def _tasks_table(task_indices: dict[str, int]) -> pa.Table:
    pandas_metadata = {
        "index_columns": ["task"],
        "column_indexes": [
            {
                "name": None,
                "field_name": None,
                "pandas_type": "unicode",
                "numpy_type": "object",
                "metadata": {"encoding": "UTF-8"},
            }
        ],
        "columns": [
            {
                "name": "task_index",
                "field_name": "task_index",
                "pandas_type": "int64",
                "numpy_type": "int64",
                "metadata": None,
            },
            {
                "name": "task",
                "field_name": "task",
                "pandas_type": "unicode",
                "numpy_type": "object",
                "metadata": None,
            },
        ],
        "attributes": {},
        "creator": {"library": "pyarrow", "version": str(pa.__version__)},
        "pandas_version": "0.0.0",
    }
    table = pa.table(
        {
            "task_index": pa.array(range(len(task_indices)), type=pa.int64()),
            "task": pa.array(list(task_indices), type=pa.string()),
        }
    )
    return table.replace_schema_metadata({b"pandas": json.dumps(pandas_metadata).encode("utf-8")})


class LeRobotSink(DataSink[LeRobotStagedShard]):
    def __init__(
        self,
        uri: str,
        fps: int,
        features: dict[str, dict[str, Any]],
        robot_type: str | None,
        overwrite: bool,
        chunks_size: int,
        data_files_size_in_mb: int,
        io_config: IOConfig | None,
        globally_sorted: bool = True,
    ) -> None:
        if not uri.strip() or uri.strip() in {"/", ".", "file:///"}:
            raise ValueError(f"Refusing to write a LeRobot dataset to unsafe path {uri!r}")
        if not isinstance(chunks_size, int) or isinstance(chunks_size, bool) or chunks_size <= 0:
            raise ValueError(f"chunks_size must be a positive integer, got {chunks_size!r}")
        if (
            not isinstance(data_files_size_in_mb, int)
            or isinstance(data_files_size_in_mb, bool)
            or data_files_size_in_mb <= 0
        ):
            raise ValueError(f"data_files_size_in_mb must be a positive integer, got {data_files_size_in_mb!r}")
        self.uri = uri.rstrip("/")
        self.fps = fps
        self._tensor_features = {
            name for name, feature in features.items() if feature.get(_ARROW_CONTAINER_KEY) == "tensor"
        }
        self._one_element_list_features = {
            name
            for name, feature in features.items()
            if feature.get(_ARROW_CONTAINER_KEY) == "fixed_size_list" and feature["shape"] == [1]
        }
        self.features = {
            name: {
                key: value
                for key, value in feature.items()
                if key not in {_ARROW_CONTAINER_KEY, _ARROW_TENSOR_TYPE_KEY}
            }
            for name, feature in features.items()
        }
        self.feature_names = list(features)
        self.robot_type = robot_type
        self.overwrite = overwrite
        self.chunks_size = chunks_size
        self.data_files_size_in_mb = data_files_size_in_mb
        self.io_config = io_config
        self.globally_sorted = globally_sorted
        self._staging_id = uuid.uuid4().hex
        self._resolved_root: str | None = None

    def name(self) -> str:
        return "LeRobot Dataset Write"

    def schema(self) -> Schema:
        return Schema.from_pydict(
            {
                "path": DataType.string(),
                "num_episodes": DataType.int64(),
                "num_frames": DataType.int64(),
                "num_tasks": DataType.int64(),
            }
        )

    def _filesystem(self) -> tuple[str, pafs.FileSystem]:
        [root], filesystem = _resolve_paths_and_filesystem(self.uri, io_config=self.io_config)
        if self._resolved_root is not None:
            return self._resolved_root, filesystem
        root = self._validate_destination(root, filesystem)
        return root, filesystem

    @staticmethod
    def _validate_destination(root: str, filesystem: pafs.FileSystem) -> str:
        if isinstance(filesystem, pafs.LocalFileSystem):
            canonical_root = os.path.realpath(root)
            canonical_cwd = os.path.realpath(os.getcwd())
            try:
                is_cwd_or_ancestor = os.path.commonpath([canonical_root, canonical_cwd]) == canonical_root
            except ValueError:
                is_cwd_or_ancestor = False
            if canonical_root == os.path.sep or is_cwd_or_ancestor:
                raise ValueError(f"Refusing to write a LeRobot dataset to unsafe local path {root!r}")

            absolute = os.path.abspath(root.rstrip(os.path.sep) or os.path.sep)
            if os.path.islink(absolute):
                raise ValueError(f"Refusing to write a LeRobot dataset through symlink path {root!r}")
            return canonical_root

        normalized = root.strip("/")
        if not normalized or "/" not in normalized:
            raise ValueError(f"Refusing to write a LeRobot dataset to object-store bucket root {root!r}")
        return root.rstrip("/")

    @staticmethod
    def _path(root: str, relative: str) -> str:
        return f"{root.rstrip('/')}/{relative.lstrip('/')}"

    def _sibling_path(self, root: str, kind: str) -> str:
        parent, _, name = root.rstrip("/").rpartition("/")
        sibling = f".{name}.daft-lr-{kind}-{self._staging_id}"
        return self._path(parent, sibling) if parent else sibling

    def _transaction_path(self, root: str) -> str:
        return self._sibling_path(root, "staging")

    def _input_staging_path(self, root: str) -> str:
        return self._path(self._transaction_path(root), ".input")

    def _validate_existing_destination(
        self, filesystem: pafs.FileSystem, root: str, destination_info: pafs.FileInfo
    ) -> None:
        if destination_info.type == pafs.FileType.Directory:
            children = filesystem.get_file_info(pafs.FileSelector(root, recursive=False))
            if not children:
                return
        marker_path = self._path(root, "meta/info.json")
        marker = filesystem.get_file_info(marker_path)
        if marker.type == pafs.FileType.File:
            try:
                with filesystem.open_input_stream(marker_path) as stream:
                    info = json.loads(stream.read())
                if not isinstance(info, dict):
                    raise TypeError("info.json must contain an object")
                version = info.get("codebase_version")
                if not isinstance(version, str) or _SUPPORTED_CODEBASE_VERSION.fullmatch(version) is None:
                    raise ValueError(f"unsupported codebase_version {version!r}")
                if not isinstance(info.get("features"), dict):
                    raise TypeError("features must be an object")
                if not isinstance(info.get("data_path"), str) or not info["data_path"]:
                    raise ValueError("data_path must be a non-empty string")
                fps = info.get("fps")
                if not isinstance(fps, (int, float)) or isinstance(fps, bool) or fps <= 0:
                    raise ValueError("fps must be positive")
                for key in ("total_episodes", "total_frames", "total_tasks"):
                    value = info.get(key)
                    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                        raise ValueError(f"{key} must be a non-negative integer")

                if info["total_episodes"] > 0:
                    major = int(version[1])
                    episodes_path = self._path(root, "meta/episodes" if major == 3 else "meta/episodes.jsonl")
                    episodes_info = filesystem.get_file_info(episodes_path)
                    expected_type = pafs.FileType.Directory if major == 3 else pafs.FileType.File
                    if episodes_info.type != expected_type:
                        raise ValueError(f"missing episode metadata at {episodes_path!r}")
                if info["total_frames"] > 0:
                    data_info = filesystem.get_file_info(self._path(root, "data"))
                    if data_info.type != pafs.FileType.Directory:
                        raise ValueError("missing data directory")
            except (json.JSONDecodeError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
                raise ValueError(
                    f"Refusing to overwrite non-LeRobot destination {self.uri!r}: invalid meta/info.json ({exc})"
                ) from exc
            return
        raise ValueError(
            f"Refusing to overwrite non-LeRobot destination {self.uri!r}; "
            "choose an empty directory or remove it explicitly"
        )

    @staticmethod
    def _delete_path(filesystem: pafs.FileSystem, path: str) -> None:
        info = filesystem.get_file_info(path)
        if info.type == pafs.FileType.Directory:
            filesystem.delete_dir(path)
        elif info.type == pafs.FileType.File:
            filesystem.delete_file(path)

    @classmethod
    def _copy_path(
        cls,
        filesystem: pafs.FileSystem,
        source: str,
        destination: str,
        *,
        commit_marker_last: bool = False,
    ) -> None:
        info = filesystem.get_file_info(source)
        if info.type == pafs.FileType.File:
            parent = destination.rsplit("/", 1)[0]
            if parent:
                filesystem.create_dir(parent, recursive=True)
            filesystem.copy_file(source, destination)
            return
        if info.type != pafs.FileType.Directory:
            raise FileNotFoundError(source)

        filesystem.create_dir(destination, recursive=True)
        selector = pafs.FileSelector(source, recursive=True)
        children = filesystem.get_file_info(selector)
        for child in children:
            relative = child.path[len(source.rstrip("/")) :].lstrip("/")
            target = cls._path(destination, relative)
            if child.type == pafs.FileType.Directory:
                filesystem.create_dir(target, recursive=True)
        files = [child for child in children if child.type == pafs.FileType.File]
        if commit_marker_last:
            files.sort(key=lambda child: child.path.endswith("/meta/info.json"))
        for child in files:
            relative = child.path[len(source.rstrip("/")) :].lstrip("/")
            target = cls._path(destination, relative)
            parent = target.rsplit("/", 1)[0]
            if parent:
                filesystem.create_dir(parent, recursive=True)
            filesystem.copy_file(child.path, target)

    def _publish(self, filesystem: pafs.FileSystem, root: str) -> None:
        transaction = self._transaction_path(root)
        backup = self._sibling_path(root, "backup")
        destination_info = filesystem.get_file_info(root)
        destination_exists = destination_info.type != pafs.FileType.NotFound
        if destination_exists and not self.overwrite:
            raise FileExistsError(
                f"LeRobot destination already exists: {self.uri!r}; pass overwrite=True to replace it"
            )
        if destination_exists:
            self._validate_existing_destination(filesystem, root, destination_info)

        if isinstance(filesystem, pafs.LocalFileSystem):
            if destination_exists:
                filesystem.move(root, backup)
            try:
                filesystem.move(transaction, root)
            except Exception:
                self._delete_path(filesystem, root)
                if destination_exists:
                    filesystem.move(backup, root)
                raise
            if destination_exists:
                try:
                    self._delete_path(filesystem, backup)
                except OSError as exc:
                    warnings.warn(f"Failed to remove LeRobot transaction backup {backup!r}: {exc}", stacklevel=2)
            return

        # Object stores generally cannot atomically rename a directory. Keep a
        # full backup until all staged files have been copied, and restore it if
        # publication fails.
        if destination_exists:
            try:
                self._copy_path(filesystem, root, backup)
            except Exception:
                # Backup creation happens before the destination is touched. If
                # it fails, remove only the partial backup and leave the intact
                # destination in place.
                try:
                    self._delete_path(filesystem, backup)
                except OSError as cleanup_error:
                    warnings.warn(f"Failed to remove partial LeRobot backup {backup!r}: {cleanup_error}", stacklevel=2)
                raise
        try:
            if destination_exists:
                self._delete_path(filesystem, root)
            self._copy_path(filesystem, transaction, root, commit_marker_last=True)
        except Exception:
            try:
                self._delete_path(filesystem, root)
                if destination_exists:
                    self._copy_path(filesystem, backup, root)
            except Exception as rollback_error:
                raise RuntimeError(
                    f"Failed to publish LeRobot dataset and rollback also failed; backup retained at {backup!r}"
                ) from rollback_error
            raise
        else:
            for cleanup_path in (transaction, backup if destination_exists else None):
                if cleanup_path is None:
                    continue
                try:
                    self._delete_path(filesystem, cleanup_path)
                except OSError as exc:
                    warnings.warn(
                        f"Failed to remove LeRobot transaction artifact {cleanup_path!r}: {exc}", stacklevel=2
                    )

    def abort(self) -> None:
        root, filesystem = self._filesystem()
        self._delete_path(filesystem, self._transaction_path(root))

    def start(self) -> None:
        [unvalidated_root], filesystem = _resolve_paths_and_filesystem(self.uri, io_config=self.io_config)
        root = self._validate_destination(unvalidated_root, filesystem)
        self._resolved_root = root
        info = filesystem.get_file_info(root)
        if info.type != pafs.FileType.NotFound and not self.overwrite:
            raise FileExistsError(
                f"LeRobot destination already exists: {self.uri!r}; pass overwrite=True to replace it"
            )
        if info.type != pafs.FileType.NotFound:
            self._validate_existing_destination(filesystem, root, info)
        transaction = self._transaction_path(root)
        self._delete_path(filesystem, transaction)
        filesystem.create_dir(self._input_staging_path(root), recursive=True)

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[LeRobotStagedShard]]:
        root, filesystem = self._filesystem()
        staging = self._input_staging_path(root)
        for partition in micropartitions:
            if len(partition) == 0:
                continue
            table = partition.to_arrow()
            for name in self._one_element_list_features:
                normalized = pc.list_element(table[name].combine_chunks(), 0)
                table = table.set_column(table.schema.get_field_index(name), name, normalized)
            for name in self._tensor_features:
                values = table[name].combine_chunks()
                if not isinstance(values, pa.ExtensionArray):
                    raise TypeError(f"Expected FixedShapeTensor storage for LeRobot feature {name!r}")
                normalized = _tensor_to_fixed_size_lists(
                    values,
                    self.features[name]["shape"],
                    _primitive_arrow_type(self.features[name]["dtype"]),
                )
                table = table.set_column(table.schema.get_field_index(name), name, normalized)
            sort_indices = pc.sort_indices(
                table, sort_keys=[("episode_index", "ascending"), ("frame_index", "ascending")]
            )
            table = table.take(sort_indices)
            fragments = [table]
            if not self.globally_sorted:
                # Native execution streams partitions without a global shuffle.
                # Stage one fragment per episode so finalize can order and merge
                # interleaved episode ranges without retaining the dataset in RAM.
                fragments = [table.slice(start, stop - start) for start, stop in _episode_ranges(table)]

            for fragment in fragments:
                first_episode = int(fragment["episode_index"][0].as_py())
                first_frame = int(fragment["frame_index"][0].as_py())
                path = self._path(staging, f"part-{uuid.uuid4().hex}.parquet")
                pq.write_table(fragment, path, filesystem=filesystem, compression="snappy", use_dictionary=True)
                size = filesystem.get_file_info(path).size
                yield WriteResult(
                    result=LeRobotStagedShard(path, first_episode, first_frame),
                    bytes_written=max(size, 0),
                    rows_written=len(fragment),
                )

    def _write_json(self, filesystem: pafs.FileSystem, path: str, value: Any) -> None:
        parent = path.rsplit("/", 1)[0]
        filesystem.create_dir(parent, recursive=True)
        with filesystem.open_output_stream(path) as stream:
            stream.write(json.dumps(_json_value(value), indent=4, ensure_ascii=False).encode("utf-8"))

    def _prepare_episode(
        self,
        table: pa.Table,
        expected_episode: int,
        dataset_from_index: int,
        task_indices: dict[str, int],
    ) -> _PreparedEpisode:
        if len(table) == 0:
            raise ValueError("LeRobot episodes must contain at least one frame")
        raw_episode_values = table["episode_index"].combine_chunks().to_pylist()
        if any(not isinstance(value, int) for value in raw_episode_values):
            raise ValueError("episode_index must contain integers and no nulls")
        episode_values = cast("list[int]", raw_episode_values)
        episode_index = episode_values[0]
        if episode_index != expected_episode or any(value != episode_index for value in episode_values):
            raise ValueError(
                "episode_index must be zero-based and contiguous; "
                f"expected episode {expected_episode}, got {episode_index}"
            )
        raw_frame_indices = table["frame_index"].combine_chunks().to_pylist()
        if any(not isinstance(value, int) for value in raw_frame_indices):
            raise ValueError("frame_index must contain integers and no nulls")
        frame_indices = cast("list[int]", raw_frame_indices)
        if frame_indices != list(range(len(table))):
            raise ValueError(
                f"frame_index for episode {episode_index} must be zero-based and contiguous; got {frame_indices[:10]}"
            )

        timestamps = table["timestamp"].combine_chunks().to_pylist()
        for frame_index, timestamp in enumerate(timestamps):
            if timestamp is None or not math.isfinite(float(timestamp)):
                raise ValueError(f"timestamp contains an invalid value in episode {episode_index}")
            expected_timestamp = struct.unpack("<f", struct.pack("<f", frame_index / self.fps))[0]
            if abs(float(timestamp) - expected_timestamp) > 1e-4:
                raise ValueError(
                    f"timestamp for episode {episode_index}, frame {frame_index} must equal frame_index / fps "
                    f"within 1e-4 seconds; got {timestamp}, expected {expected_timestamp}"
                )

        raw_tasks = table[_TASK_COLUMN].combine_chunks().to_pylist()
        if any(not isinstance(task, str) or not task for task in raw_tasks):
            raise ValueError("task_column must contain non-empty strings and no nulls")
        tasks = cast("list[str]", raw_tasks)
        for task in tasks:
            if task not in task_indices:
                task_indices[task] = len(task_indices)

        canonical_arrays: dict[str, pa.Array | pa.ChunkedArray] = {
            "timestamp": pc.cast(table["timestamp"], pa.float32()),
            "frame_index": pa.chunked_array([pa.array(frame_indices, type=pa.int64())]),
            "episode_index": pa.chunked_array([pa.array(episode_values, type=pa.int64())]),
            "index": pa.chunked_array(
                [pa.array(range(dataset_from_index, dataset_from_index + len(table)), type=pa.int64())]
            ),
            "task_index": pa.chunked_array([pa.array([task_indices[task] for task in tasks], type=pa.int64())]),
        }
        output_names = [*self.feature_names, *canonical_arrays]
        output_arrays = [*[table[name] for name in self.feature_names], *canonical_arrays.values()]
        output = pa.Table.from_arrays(output_arrays, names=output_names)
        all_features = {**self.features, **_CANONICAL_FEATURES}
        stats = _feature_stats(output, all_features)
        episode_tasks = sorted(set(tasks), key=task_indices.__getitem__)
        metadata: dict[str, Any] = {
            "episode_index": episode_index,
            "tasks": episode_tasks,
            "length": len(table),
            "dataset_from_index": dataset_from_index,
            "dataset_to_index": dataset_from_index + len(table),
        }
        for feature_name, feature_stats in stats.items():
            for stat_name, value in feature_stats.items():
                metadata[f"stats/{feature_name}/{stat_name}"] = value
        return _PreparedEpisode(output, metadata, stats)

    def _write_final_shard(
        self,
        filesystem: pafs.FileSystem,
        root: str,
        episodes: list[_PreparedEpisode],
        file_number: int,
    ) -> None:
        chunk_index = file_number // self.chunks_size
        file_index = file_number % self.chunks_size
        data_relative = _DATA_PATH.format(chunk_index=chunk_index, file_index=file_index)
        data_path = self._path(root, data_relative)
        filesystem.create_dir(data_path.rsplit("/", 1)[0], recursive=True)
        writer: pq.ParquetWriter | None = None
        try:
            for episode in episodes:
                if writer is None:
                    writer = pq.ParquetWriter(
                        data_path,
                        episode.table.schema,
                        filesystem=filesystem,
                        compression="snappy",
                        use_dictionary=True,
                    )
                writer.write_table(episode.table)
        finally:
            if writer is not None:
                writer.close()

        episode_relative = _EPISODES_PATH.format(chunk_index=chunk_index, file_index=file_index)
        episode_path = self._path(root, episode_relative)
        filesystem.create_dir(episode_path.rsplit("/", 1)[0], recursive=True)
        metadata_rows = []
        for episode in episodes:
            metadata_rows.append(
                {
                    **episode.metadata,
                    "data/chunk_index": chunk_index,
                    "data/file_index": file_index,
                    "meta/episodes/chunk_index": chunk_index,
                    "meta/episodes/file_index": file_index,
                }
            )
        pq.write_table(
            pa.Table.from_pylist(metadata_rows),
            episode_path,
            filesystem=filesystem,
            compression="snappy",
            use_dictionary=True,
        )

    def finalize(self, write_results: list[WriteResult[LeRobotStagedShard]]) -> MicroPartition:
        root, filesystem = self._filesystem()
        transaction = self._transaction_path(root)
        staging = self._input_staging_path(root)
        task_indices: dict[str, int] = {}
        global_stats: dict[str, dict[str, list[int | float]]] | None = None
        expected_episode = 0
        dataset_from_index = 0
        pending_table: pa.Table | None = None
        buffered: list[_PreparedEpisode] = []
        buffered_bytes = 0
        file_number = 0
        target_bytes = self.data_files_size_in_mb * 1024 * 1024

        def flush_buffer() -> None:
            nonlocal buffered, buffered_bytes, file_number
            if not buffered:
                return
            self._write_final_shard(filesystem, transaction, buffered, file_number)
            file_number += 1
            buffered = []
            buffered_bytes = 0

        def consume_episode(table: pa.Table) -> None:
            nonlocal expected_episode, dataset_from_index, global_stats, buffered_bytes
            # A single episode can be split across several distributed write tasks.
            # Sort again after concatenation rather than relying on task completion
            # order (or on non-overlapping partition ranges).
            table = table.take(
                pc.sort_indices(table, sort_keys=[("episode_index", "ascending"), ("frame_index", "ascending")])
            )
            episode = self._prepare_episode(table, expected_episode, dataset_from_index, task_indices)
            if buffered and buffered_bytes + episode.table.nbytes > target_bytes:
                flush_buffer()
            buffered.append(episode)
            buffered_bytes += episode.table.nbytes
            global_stats = _aggregate_stats(global_stats, episode.stats)
            expected_episode += 1
            dataset_from_index += len(episode.table)

        try:
            if not write_results:
                raise ValueError("Cannot write an empty LeRobot dataset")

            shards = sorted(
                (result.result for result in write_results),
                key=lambda result: (result.min_episode_index, result.min_frame_index, result.path),
            )
            for shard in shards:
                table = pq.read_table(shard.path, filesystem=filesystem)
                sort_indices = pc.sort_indices(
                    table, sort_keys=[("episode_index", "ascending"), ("frame_index", "ascending")]
                )
                table = table.take(sort_indices)
                for start, stop in _episode_ranges(table):
                    current = table.slice(start, stop - start)
                    current_episode = int(current["episode_index"][0].as_py())
                    if pending_table is None:
                        pending_table = current
                    elif int(pending_table["episode_index"][0].as_py()) == current_episode:
                        pending_table = pa.concat_tables([pending_table, current])
                    else:
                        consume_episode(pending_table)
                        pending_table = current
            if pending_table is not None:
                consume_episode(pending_table)
            flush_buffer()

            task_table = _tasks_table(task_indices)
            tasks_path = self._path(transaction, "meta/tasks.parquet")
            filesystem.create_dir(tasks_path.rsplit("/", 1)[0], recursive=True)
            pq.write_table(task_table, tasks_path, filesystem=filesystem, compression="snappy")

            assert global_stats is not None
            self._write_json(filesystem, self._path(transaction, "meta/stats.json"), global_stats)
            info = {
                "codebase_version": "v3.0",
                "robot_type": self.robot_type,
                "total_episodes": expected_episode,
                "total_frames": dataset_from_index,
                "total_tasks": len(task_indices),
                "chunks_size": self.chunks_size,
                "data_files_size_in_mb": self.data_files_size_in_mb,
                "video_files_size_in_mb": 200,
                "fps": self.fps,
                "splits": {"train": f"0:{expected_episode}"},
                "data_path": _DATA_PATH,
                "video_path": None,
                "features": {**self.features, **_CANONICAL_FEATURES},
            }
            self._write_json(filesystem, self._path(transaction, "meta/info.json"), info)
            self._delete_path(filesystem, staging)
            self._publish(filesystem, root)
        except Exception:
            self.abort()
            raise

        return MicroPartition.from_pydict(
            {
                "path": [self.uri],
                "num_episodes": pa.array([expected_episode], type=pa.int64()),
                "num_frames": pa.array([dataset_from_index], type=pa.int64()),
                "num_tasks": pa.array([len(task_indices)], type=pa.int64()),
            }
        )


__all__ = ["LeRobotSink", "feature_arrow_type", "normalize_lerobot_features"]
