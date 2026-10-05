from __future__ import annotations

import io
import json
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from PIL import Image

import daft
from daft import DataType, IOConfig, col
from daft.io.huggingface._features import decode_huggingface_images, parquet_features

_STORAGE = pa.struct([pa.field("bytes", pa.binary()), pa.field("path", pa.string())])


@pytest.fixture
def png_bytes():
    buffer = io.BytesIO()
    Image.new("RGB", (3, 2), "red").save(buffer, format="PNG")
    return buffer.getvalue()


def _table(data, features=None, *, schema=None):
    table = pa.Table.from_pydict(data, schema=schema)
    if features is not None:
        # The HF key is deliberately not the first metadata key.
        table = table.replace_schema_metadata(
            {b"unrelated": b"value", b"huggingface": json.dumps({"info": {"features": features}}).encode()}
        )
    return table


def test_image_bytes_are_decoded_lazily_without_opening_display_paths(tmp_path, png_bytes):
    path = tmp_path / "data.parquet"
    table = _table(
        {"image": [{"bytes": png_bytes, "path": "does-not-exist.png"}, None, {"bytes": None, "path": None}]},
        {"image": {"_type": "Image"}},
        schema=pa.schema([pa.field("image", _STORAGE)]),
    )
    pq.write_table(table, path)
    config = IOConfig()
    df = decode_huggingface_images(
        daft.read_parquet(str(path)), parquet_features([str(path)], config), tmp_path.as_uri(), config
    )
    assert df.schema()["image"].dtype == DataType.image()
    assert df.select(col("image").image_width().alias("width")).to_pydict() == {"width": [3, None, None]}


def test_image_paths_use_dataset_root_and_null_embedded_bytes(tmp_path, png_bytes):
    (tmp_path / "image.png").write_bytes(png_bytes)
    table = _table(
        {"image": [{"bytes": None, "path": "image.png"}]},
        schema=pa.schema([pa.field("image", _STORAGE)]),
    )
    df = decode_huggingface_images(daft.from_arrow(table), {"image": {"_type": "Image"}}, tmp_path.as_uri(), IOConfig())
    assert df.select(col("image").image_height().alias("height")).to_pydict() == {"height": [2]}


def test_image_storage_field_order_is_not_significant(png_bytes):
    reversed_storage = pa.struct([pa.field("path", pa.large_string()), pa.field("bytes", pa.large_binary())])
    table = _table(
        {"image": [{"bytes": png_bytes, "path": "display.png"}]},
        schema=pa.schema([pa.field("image", reversed_storage)]),
    )
    df = decode_huggingface_images(
        daft.from_arrow(table), {"image": {"_type": "Image"}}, "hf://datasets/org/repo", IOConfig()
    )
    assert df.select(col("image").image_width().alias("width")).to_pydict() == {"width": [3]}


def test_multiple_and_nested_images_preserve_other_fields_and_null_parent(png_bytes):
    table = _table(
        {
            "scene": [{"photo": {"bytes": png_bytes, "path": None}, "label": "hello"}, None],
            "other": [{"bytes": png_bytes, "path": None}, None],
        },
        schema=pa.schema(
            [
                pa.field("scene", pa.struct([pa.field("photo", _STORAGE), pa.field("label", pa.string())])),
                pa.field("other", _STORAGE),
            ]
        ),
    )
    features = {
        "scene": {"photo": {"_type": "Image"}, "label": {"_type": "Value", "dtype": "string"}},
        "other": {"_type": "Image"},
    }
    df = decode_huggingface_images(daft.from_arrow(table), features, "hf://datasets/org/repo", IOConfig())
    assert df.select(
        col("scene").get("photo").image_width().alias("width"),
        col("scene").get("label"),
        col("other").image_height().alias("height"),
    ).to_pydict() == {
        "width": [3, None],
        "label": ["hello", None],
        "height": [2, None],
    }


def test_unannotated_structs_are_unchanged(tmp_path, png_bytes):
    path = tmp_path / "ordinary.parquet"
    pq.write_table(
        _table({"image": [{"bytes": png_bytes, "path": None}]}, schema=pa.schema([pa.field("image", _STORAGE)])), path
    )
    df = daft.read_parquet(str(path))
    assert (
        decode_huggingface_images(df, parquet_features([str(path)], IOConfig()), "hf://datasets/org/repo", IOConfig())
        is df
    )


def test_inconsistent_image_flags_across_shards_are_rejected(tmp_path, png_bytes):
    files = []
    for index, features in enumerate([{"image": {"_type": "Image"}}, {}]):
        path = tmp_path / f"{index}.parquet"
        pq.write_table(_table({"image": [{"bytes": png_bytes, "path": None}]}, features), path)
        files.append(str(path))
    with pytest.raises(ValueError, match="inconsistent"):
        parquet_features(files, IOConfig())


def test_malformed_hf_metadata_is_reported(tmp_path):
    path = tmp_path / "bad.parquet"
    pq.write_table(pa.table({"value": [1]}).replace_schema_metadata({b"huggingface": b"not-json"}), path)
    with pytest.raises(ValueError, match="Invalid Hugging Face feature metadata"):
        parquet_features([str(path)], IOConfig())


def test_image_flag_requires_correct_physical_schema():
    with pytest.raises(ValueError, match="bytes/path struct"):
        decode_huggingface_images(
            daft.from_pydict({"image": ["not a struct"]}),
            {"image": {"_type": "Image"}},
            "hf://datasets/org/repo",
            IOConfig(),
        )


def test_missing_image_field_is_reported():
    with pytest.raises(ValueError, match="missing columns"):
        decode_huggingface_images(
            daft.from_pydict({"value": [1]}), {"image": {"_type": "Image"}}, "hf://datasets/org/repo", IOConfig()
        )


def test_missing_nested_image_field_is_reported():
    with pytest.raises(ValueError, match="missing nested field"):
        decode_huggingface_images(
            daft.from_pydict({"scene": [{"label": "ordinary"}]}),
            {"scene": {"image": {"_type": "Image"}}},
            "hf://datasets/org/repo",
            IOConfig(),
        )


def test_unused_image_projection_does_not_decode_invalid_payload():
    table = _table(
        {"image": [{"bytes": b"invalid", "path": None}], "value": [1]},
        schema=pa.schema([pa.field("image", _STORAGE), pa.field("value", pa.int64())]),
    )
    df = decode_huggingface_images(
        daft.from_arrow(table), {"image": {"_type": "Image"}}, "hf://datasets/org/repo", IOConfig()
    )
    assert df.select("value").to_pydict() == {"value": [1]}


def test_relative_image_url_uses_requested_revision_and_preserves_io_config(png_bytes):
    table = _table(
        {
            "image": [
                {"bytes": None, "path": "images/a.png"},
                {"bytes": None, "path": "https://example.com/b.png"},
                {"bytes": png_bytes, "path": "display-name.png"},
            ]
        },
        schema=pa.schema([pa.field("image", _STORAGE)]),
    )
    raw = daft.from_arrow(table)
    config = IOConfig()

    def download(expr, **kwargs):
        assert kwargs["io_config"] is config
        assert kwargs["on_error"] == "raise"
        assert raw.select(expr.alias("url")).to_pydict() == {
            "url": ["hf://datasets/org/repo@old/images/a.png", "https://example.com/b.png", None]
        }
        return daft.lit(png_bytes)

    with patch("daft.expressions.Expression.download", autospec=True, side_effect=download):
        df = decode_huggingface_images(raw, {"image": {"_type": "Image"}}, "hf://datasets/org/repo@old", config)
    assert df.select(col("image").image_width().alias("width")).to_pydict() == {"width": [3, 3, 3]}


def test_datasets_fallback_image_features(png_bytes):
    from datasets import Dataset, Features
    from datasets import Image as HFImage

    dataset = Dataset.from_dict(
        {"image": [{"bytes": png_bytes, "path": None}]}, features=Features({"image": HFImage()})
    )
    with (
        patch("datasets.load_dataset", return_value=dataset),
        patch("daft.io.huggingface.warn_if_lerobot"),
        pytest.warns(UserWarning, match="materialize"),
    ):
        df = daft.read_huggingface("org/repo", format="datasets", split="train", decode_images=True)
    assert df.select(col("image").image_height().alias("height")).to_pydict() == {"height": [2]}


@pytest.mark.parametrize("feature", [[{"_type": "Image"}], {"_type": "List", "feature": {"_type": "Image"}}])
def test_sequence_images_are_explicitly_unsupported(feature):
    with pytest.raises(NotImplementedError, match="sequence"):
        decode_huggingface_images(
            daft.from_pydict({"images": [1]}), {"images": feature}, "hf://datasets/org/repo", IOConfig()
        )


def test_public_reader_raw_default_and_opt_in(tmp_path, png_bytes):
    path = tmp_path / "images.parquet"
    table = _table(
        {"image": [{"bytes": png_bytes, "path": None}]},
        {"image": {"_type": "Image"}},
        schema=pa.schema([pa.field("image", _STORAGE)]),
    )
    pq.write_table(table, path)
    with (
        patch("daft.io.huggingface.parquet_files", return_value=[str(path)]),
        patch("daft.io.huggingface.warn_if_lerobot"),
    ):
        raw = daft.read_huggingface("org/repo")
        decoded = daft.read_huggingface("org/repo", decode_images=True)
    assert raw.schema()["image"].dtype == DataType.struct({"bytes": DataType.binary(), "path": DataType.string()})
    assert decoded.select(col("image").image_width().alias("width")).to_pydict() == {"width": [3]}
