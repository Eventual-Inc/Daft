from __future__ import annotations

import os
import sys
from unittest.mock import patch

import pytest

from daft.io.delta_lake.utils import construct_delta_file_path


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Unix-specific test")
@pytest.mark.parametrize(
    "scheme,table_uri,relative_path,expected",
    [
        # Object storage schemes (unaffected by OS)
        ("s3", "s3://bucket/table", "data/file.parquet", "s3://bucket/table/data/file.parquet"),
        ("s3a", "s3a://bucket/table/", "data/file.parquet", "s3a://bucket/table/data/file.parquet"),
        # Local file system - Unix style
        ("file", "/tmp/table", "data/file.parquet", os.path.join("/tmp/table", "data/file.parquet")),
        (
            "hdfs",
            "hdfs://namenode/table",
            "data/file.parquet",
            os.path.join("hdfs://namenode/table", "data/file.parquet"),
        ),
    ],
)
def test_construct_delta_file_path(scheme, table_uri, relative_path, expected):
    result = construct_delta_file_path(scheme, table_uri, relative_path)
    assert result == expected


@pytest.mark.parametrize(
    "scheme,table_uri,relative_path,expected",
    [
        # Object storage schemes should always use forward slashes
        ("s3", "s3://bucket/table", "data/file.parquet", "s3://bucket/table/data/file.parquet"),
        ("s3a", "s3a://bucket/table/", "data/file.parquet", "s3a://bucket/table/data/file.parquet"),
        ("gcs", "gs://bucket/table", "_delta_log/00000.json", "gs://bucket/table/_delta_log/00000.json"),
        ("gs", "gs://bucket/table", "data/file.parquet", "gs://bucket/table/data/file.parquet"),
        ("az", "az://container/table", "data/file.parquet", "az://container/table/data/file.parquet"),
        ("abfs", "abfs://container/table", "data/file.parquet", "abfs://container/table/data/file.parquet"),
        ("abfss", "abfss://container/table", "data/file.parquet", "abfss://container/table/data/file.parquet"),
    ],
)
def test_object_storage_uses_forward_slashes_on_windows(scheme, table_uri, relative_path, expected):
    # Mock os.path.join to return Windows-style paths with backslashes
    with patch("os.path.join", return_value="C:\\fake\\windows\\path"):
        result = construct_delta_file_path(scheme, table_uri, relative_path)
        assert result == expected
        assert "\\" not in result  # Ensure no backslashes in object storage paths
