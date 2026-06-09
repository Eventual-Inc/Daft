from __future__ import annotations

from unittest.mock import patch

import pyarrow.fs as pafs
import pytest

import daft.filesystem as fs_mod
from daft.io import IOConfig, S3Config


@pytest.fixture(autouse=True)
def clear_fs_cache():
    fs_mod._CACHED_FSES.clear()
    yield
    fs_mod._CACHED_FSES.clear()


def test_cache_hits_for_semantically_equal_io_configs():
    """Two separately constructed but semantically-equal IOConfigs must reuse one cached filesystem.

    Regression test: IOConfig.__eq__ is identity-based on the PyO3 wrapper, so the original
    cache key (protocol, IOConfig) missed on every call when the Rust side handed a fresh
    Python wrapper to each writer. That caused per-writer S3FileSystem rebuilds and the
    file-descriptor / thread-pool leak in long-running write_iceberg jobs.
    """
    cfg_a = IOConfig(s3=S3Config(region_name="us-east-1", endpoint_url="http://example"))
    cfg_b = IOConfig(s3=S3Config(region_name="us-east-1", endpoint_url="http://example"))

    assert cfg_a is not cfg_b
    # IOConfig.__eq__ is identity-based; this is the bug we're working around.
    assert cfg_a != cfg_b

    with patch.object(fs_mod, "_build_filesystem", wraps=fs_mod._build_filesystem) as build:
        _, fs1 = fs_mod._resolve_paths_and_filesystem("/tmp", io_config=cfg_a)
        _, fs2 = fs_mod._resolve_paths_and_filesystem("/tmp", io_config=cfg_b)

    assert fs1 is fs2, "cache must return the same filesystem instance for equal IOConfigs"
    assert build.call_count == 1, f"expected one _build_filesystem call, got {build.call_count}"


def test_cache_misses_for_distinct_io_configs():
    """Sanity check: configs with different content do not collide on the cache key."""
    cfg_us = IOConfig(s3=S3Config(region_name="us-east-1"))
    cfg_eu = IOConfig(s3=S3Config(region_name="eu-west-1"))

    with patch.object(fs_mod, "_build_filesystem", wraps=fs_mod._build_filesystem) as build:
        _, fs_us = fs_mod._resolve_paths_and_filesystem("/tmp", io_config=cfg_us)
        _, fs_eu = fs_mod._resolve_paths_and_filesystem("/tmp", io_config=cfg_eu)

    assert fs_us is not fs_eu
    assert build.call_count == 2


def test_cache_hits_for_none_io_config():
    """Repeated calls with io_config=None must hit the cache."""
    with patch.object(fs_mod, "_build_filesystem", wraps=fs_mod._build_filesystem) as build:
        _, fs1 = fs_mod._resolve_paths_and_filesystem("/tmp", io_config=None)
        _, fs2 = fs_mod._resolve_paths_and_filesystem("/tmp", io_config=None)

    assert fs1 is fs2
    assert build.call_count == 1
    assert isinstance(fs1, pafs.LocalFileSystem)
