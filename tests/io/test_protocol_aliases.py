"""Tests for protocol aliases in IOConfig."""

from __future__ import annotations

import pickle
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft
from daft.daft import IOConfig

# ---------------------------------------------------------------------------
# Config-level tests
# ---------------------------------------------------------------------------


def test_default_has_empty_protocol_aliases():
    config = IOConfig()
    assert config.protocol_aliases == {}


def test_set_and_retrieve_aliases():
    config = IOConfig(protocol_aliases={"my-s3": "s3", "company-store": "gcs"})
    assert config.protocol_aliases == {"my-s3": "s3", "company-store": "gcs"}


def test_case_normalization():
    config = IOConfig(protocol_aliases={"MY-S3": "S3", "Company-Store": "GCS"})
    assert config.protocol_aliases == {"my-s3": "s3", "company-store": "gcs"}


def test_replace_replaces_aliases():
    config = IOConfig(protocol_aliases={"a": "s3"})
    replaced = config.replace(protocol_aliases={"b": "gcs"})
    assert replaced.protocol_aliases == {"b": "gcs"}


def test_replace_preserves_aliases_when_omitted():
    config = IOConfig(protocol_aliases={"a": "s3"})
    replaced = config.replace()
    assert replaced.protocol_aliases == {"a": "s3"}


def test_pickle_roundtrip():
    config = IOConfig(protocol_aliases={"my-s3": "s3", "custom": "gcs"})
    restored = pickle.loads(pickle.dumps(config))
    assert restored.protocol_aliases == config.protocol_aliases


def test_hash_includes_aliases():
    config_a = IOConfig(protocol_aliases={"my-s3": "s3"})
    config_b = IOConfig(protocol_aliases={"my-s3": "s3"})
    config_c = IOConfig(protocol_aliases={"other": "gcs"})
    assert hash(config_a) == hash(config_b)
    assert hash(config_a) != hash(config_c)


def test_rejects_builtin_scheme_as_alias_key():
    with pytest.raises(ValueError, match="conflicts with built-in scheme"):
        IOConfig(protocol_aliases={"s3": "gcs"})


def test_rejects_builtin_scheme_via_replace():
    config = IOConfig()
    with pytest.raises(ValueError, match="conflicts with built-in scheme"):
        config.replace(protocol_aliases={"az": "s3"})


# ---------------------------------------------------------------------------
# Integration tests using OpenDAL fs backend
# ---------------------------------------------------------------------------


@pytest.fixture
def parquet_data(tmp_path):
    """Create a temporary parquet file with sample data."""
    table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    papq.write_table(table, str(tmp_path / "data.parquet"))
    return tmp_path


def _alias_fs_io_config(root_dir: Path) -> IOConfig:
    """Create IOConfig that aliases 'myfs' -> 'fs' with OpenDAL fs backend."""
    return IOConfig(
        opendal_backends={"fs": {"root": str(root_dir)}},
        protocol_aliases={"myfs": "fs"},
    )


def test_alias_to_fs_reads_parquet(parquet_data):
    """Test that an alias to the fs backend reads parquet correctly."""
    io_config = _alias_fs_io_config(parquet_data)
    df = daft.read_parquet("myfs://localhost/data.parquet", io_config=io_config)
    result = df.collect()
    assert result.to_pydict() == {"x": [1, 2, 3], "y": ["a", "b", "c"]}


def test_alias_to_fs_write_and_read(tmp_path):
    """Test that an alias to the fs backend can write and read back."""
    io_config = _alias_fs_io_config(tmp_path)
    df = daft.from_pydict({"a": [10, 20, 30], "b": ["x", "y", "z"]})
    df.write_parquet("myfs://localhost/out", io_config=io_config)

    result = daft.read_parquet("myfs://localhost/out/*.parquet", io_config=io_config).sort("a").collect()
    assert result.to_pydict() == {"a": [10, 20, 30], "b": ["x", "y", "z"]}
