from __future__ import annotations

from daft.io import IOConfig, S3Config
from daft.io.iceberg._iceberg import _enable_oss_io_config


def test_enable_oss_io_config_oss_location():
    """An oss:// table location enables virtual-hosted addressing and the oss->s3 alias."""
    io_config = IOConfig(s3=S3Config(endpoint_url="http://oss-cn-hangzhou.aliyuncs.com", key_id="ak"))
    result = _enable_oss_io_config(io_config, "oss://my-bucket/warehouse/db/table")
    assert result is not None
    assert result.s3.force_virtual_addressing is True
    assert result.protocol_aliases == {"oss": "s3"}
    # Unrelated S3 settings are preserved.
    assert result.s3.endpoint_url == "http://oss-cn-hangzhou.aliyuncs.com"
    assert result.s3.key_id == "ak"


def test_enable_oss_io_config_non_oss_location_unchanged():
    """A non-oss:// table location leaves the IOConfig untouched."""
    io_config = IOConfig(s3=S3Config(endpoint_url="https://s3.us-west-2.amazonaws.com"))
    assert _enable_oss_io_config(io_config, "s3://my-bucket/warehouse/db/table") is io_config


def test_enable_oss_io_config_none():
    """A None IOConfig passes through unchanged."""
    assert _enable_oss_io_config(None, "oss://my-bucket/table") is None
