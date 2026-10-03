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


@pytest.mark.parametrize("with_config", [False, True])
@pytest.mark.parametrize(
    "global_endpoint,s3_endpoint,ignore,expected",
    [
        (None, None, None, None),
        ("http://global:9000", None, None, "http://global:9000"),
        (None, "http://s3:9000", None, "http://s3:9000"),
        ("http://global:9000", "http://s3:9000", None, "http://s3:9000"),
        ("http://global:9000", "", "false", "http://global:9000"),
        ("", "", None, None),
        ("http://global:9000", "http://s3:9000", "TrUe", None),
    ],
)
def test_s3_endpoint_environment(monkeypatch, with_config, global_endpoint, s3_endpoint, ignore, expected):
    for name, value in (
        ("AWS_ENDPOINT_URL", global_endpoint),
        ("AWS_ENDPOINT_URL_S3", s3_endpoint),
        ("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", ignore),
    ):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    config = IOConfig() if with_config else None
    with patch.object(fs_mod.pafs, "S3FileSystem") as constructor:
        fs_mod._resolve_paths_and_filesystem("s3://bucket/path", config)
    assert constructor.call_args.kwargs.get("endpoint_override") == expected


@pytest.mark.parametrize("ignore", ["true", "false"])
def test_s3_explicit_endpoint_overrides_environment(monkeypatch, ignore):
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://global:9000")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://s3:9000")
    monkeypatch.setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", ignore)
    config = IOConfig(s3=S3Config(endpoint_url="http://explicit:9000"))
    with patch.object(fs_mod.pafs, "S3FileSystem") as constructor:
        fs_mod._resolve_paths_and_filesystem("s3://bucket/path", config)
    assert constructor.call_args.kwargs["endpoint_override"] == "http://explicit:9000"


@pytest.mark.parametrize("protocol", ["s3", "s3a", "s3n"])
@pytest.mark.parametrize("with_config", [False, True])
def test_s3_cache_tracks_endpoint_environment(monkeypatch, protocol, with_config):
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
    monkeypatch.delenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", raising=False)
    config = IOConfig() if with_config else None
    with patch.object(fs_mod.pafs, "S3FileSystem", side_effect=lambda **kwargs: pafs.LocalFileSystem()) as constructor:
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://first:9000")
        _, first = fs_mod._resolve_paths_and_filesystem(f"{protocol}://bucket/path", config)
        _, cached = fs_mod._resolve_paths_and_filesystem(f"{protocol}://bucket/path", config)
        assert cached is first

        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://second:9000")
        _, second = fs_mod._resolve_paths_and_filesystem(f"{protocol}://bucket/path", config)
        assert second is not first

        monkeypatch.setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "true")
        _, default = fs_mod._resolve_paths_and_filesystem(f"{protocol}://bucket/path", config)
        assert default is not second

    assert [call.kwargs.get("endpoint_override") for call in constructor.call_args_list] == [
        "http://first:9000",
        "http://second:9000",
        None,
    ]


def test_s3_explicit_endpoint_cache_ignores_environment_changes(monkeypatch):
    config = IOConfig(s3=S3Config(endpoint_url="http://explicit:9000"))
    with patch.object(fs_mod.pafs, "S3FileSystem", side_effect=lambda **kwargs: pafs.LocalFileSystem()) as constructor:
        monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://first:9000")
        _, first = fs_mod._resolve_paths_and_filesystem("s3://bucket/path", config)
        monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://second:9000")
        _, second = fs_mod._resolve_paths_and_filesystem("s3://bucket/path", config)
    assert first is second
    assert constructor.call_count == 1


@pytest.mark.parametrize("change_during", ["lookup", "build"])
@pytest.mark.parametrize("initial_endpoint", [None, "http://first:9000"])
def test_s3_cache_preserves_endpoint_snapshot(monkeypatch, change_during, initial_endpoint):
    monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
    monkeypatch.delenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", raising=False)
    if initial_endpoint is None:
        monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    else:
        monkeypatch.setenv("AWS_ENDPOINT_URL", initial_endpoint)
    lookup = fs_mod._get_fs_from_cache

    def change_after_lookup(*args, **kwargs):
        result = lookup(*args, **kwargs)
        if change_during == "lookup":
            monkeypatch.setenv("AWS_ENDPOINT_URL", "http://second:9000")
        return result

    def build(**kwargs):
        if change_during == "build":
            monkeypatch.setenv("AWS_ENDPOINT_URL", "http://second:9000")
        return pafs.LocalFileSystem()

    with patch.object(fs_mod.pafs, "S3FileSystem", side_effect=build) as constructor:
        with patch.object(fs_mod, "_get_fs_from_cache", side_effect=change_after_lookup):
            _, first = fs_mod._resolve_paths_and_filesystem("s3://bucket/path")
        assert constructor.call_args.kwargs.get("endpoint_override") == initial_endpoint

        _, second = fs_mod._resolve_paths_and_filesystem("s3://bucket/path")
        assert second is not first
        assert constructor.call_args.kwargs["endpoint_override"] == "http://second:9000"

        if initial_endpoint is None:
            monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
        else:
            monkeypatch.setenv("AWS_ENDPOINT_URL", initial_endpoint)
        _, cached = fs_mod._resolve_paths_and_filesystem("s3://bucket/path")
        assert cached is first
    assert constructor.call_count == 2
