from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from daft.datatype import DataType
from daft.dependencies import pa, pq
from daft.expressions import col, lit
from daft.file import open_file
from daft.functions import coalesce, decode_image, startswith, to_struct, when

if TYPE_CHECKING:
    from daft import DataFrame
    from daft.daft import IOConfig
    from daft.expressions import Expression


def _contains_image(feature: Any) -> bool:
    if isinstance(feature, dict):
        return feature.get("_type") == "Image" or any(_contains_image(value) for value in feature.values())
    return isinstance(feature, list) and any(_contains_image(value) for value in feature)


def _image_paths(features: Any, prefix: tuple[str, ...] = ()) -> set[tuple[str, ...]]:
    if not isinstance(features, dict):
        if _contains_image(features):
            raise NotImplementedError("decode_images does not yet support sequences of HF images; retain raw structs")
        return set()
    if features.get("_type") == "Image":
        return {prefix}
    if isinstance(features.get("_type"), str):
        if _contains_image(features):
            raise NotImplementedError(
                "decode_images does not yet support wrapped/sequence HF images; retain raw structs"
            )
        return set()
    return {path for name, feature in features.items() for path in _image_paths(feature, (*prefix, name))}


def parquet_features(files: list[str], io_config: IOConfig) -> dict[str, Any]:
    """Read the named HF footer key and check image annotations across selected shards."""
    first_features: dict[str, Any] | None = None
    first_paths: set[tuple[str, ...]] | None = None
    for path in files:
        with open_file(path, "rb", io_config=io_config) as handle:
            metadata = pq.ParquetFile(handle).schema_arrow.metadata or {}
        try:
            info = json.loads(metadata[b"huggingface"]) if b"huggingface" in metadata else {}
            features = info.get("info", {}).get("features", {})
            if not isinstance(features, dict):
                raise TypeError("features must be an object")
            paths = _image_paths(features)
        except (ValueError, TypeError, AttributeError) as error:
            raise ValueError(f"Invalid Hugging Face feature metadata in {path!r}") from error
        if first_features is None:
            first_features, first_paths = features, paths
        elif paths != first_paths:
            raise ValueError("Hugging Face image feature metadata is inconsistent across the selected Parquet shards")
    return first_features or {}


def decode_huggingface_images(df: DataFrame, features: dict[str, Any], root: str, io_config: IOConfig) -> DataFrame:
    """Build ordinary lazy projections; never decode rows during metadata discovery."""
    image_paths = _image_paths(features)
    if not image_paths:
        return df

    def convert(expr: Expression, dtype: DataType, path: tuple[str, ...]) -> Expression:
        physical = dtype.to_arrow_dtype()
        if path in image_paths:
            if (
                not pa.types.is_struct(physical)
                or set(physical.names) != {"bytes", "path"}
                or DataType.from_arrow_type(physical.field("bytes").type) != DataType.binary()
                or DataType.from_arrow_type(physical.field("path").type) != DataType.string()
            ):
                raise ValueError(f"HF Image feature {'.'.join(path)!r} requires a bytes/path struct, got {dtype}")
            embedded = expr.get("bytes")
            location = expr.get("path")
            absolute = (
                startswith(location, "https://") | startswith(location, "http://") | startswith(location, "hf://")
            )
            location = when(absolute, location).otherwise(lit(root.rstrip("/") + "/") + location)
            # coalesce evaluates children, so null the location *before* downloading.
            # HF frequently stores a display path alongside embedded bytes.
            location = when(embedded.is_null(), location).otherwise(lit(None).cast(DataType.string()))
            contents = coalesce(embedded, location.download(io_config=io_config, on_error="raise"))
            return decode_image(contents, mode=None)
        if pa.types.is_struct(physical) and any(candidate[: len(path)] == path for candidate in image_paths):
            rebuilt = to_struct(
                *[
                    convert(expr.get(field.name), DataType.from_arrow_type(field.type), (*path, field.name)).alias(
                        field.name
                    )
                    for field in physical
                ]
            )
            return when(expr.is_null(), lit(None)).otherwise(rebuilt)
        return expr

    missing = {path[0] for path in image_paths} - set(df.column_names)
    if missing:
        raise ValueError(f"HF image features refer to missing columns: {sorted(missing)}")
    for path in image_paths:
        physical = df.schema()[path[0]].dtype.to_arrow_dtype()
        for name in path[1:]:
            if not pa.types.is_struct(physical) or name not in physical.names:
                raise ValueError(f"HF image feature {'.'.join(path)!r} refers to a missing nested field")
            physical = physical.field(name).type
    columns = [
        convert(col(field.name), field.dtype, (field.name,)).alias(field.name)
        for field in df.schema()
        if any(path[0] == field.name for path in image_paths)
    ]
    return df.with_columns({expr.name(): expr for expr in columns})
