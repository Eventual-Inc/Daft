from __future__ import annotations

from daft import context
from daft.io import IOConfig, S3Config
from daft.io.iceberg._iceberg import (
    _convert_iceberg_file_io_properties_to_io_config,
    resolve_iceberg_io_config,
)


class _FakeFileIO:
    def __init__(self, properties: dict[str, str]):
        self.properties = properties


class _FakeTable:
    """Minimal stand-in for a PyIceberg Table exposing only what the resolver reads."""

    def __init__(self, properties: dict[str, str], location: str):
        self.io = _FakeFileIO(properties)
        self._location = location

    def location(self) -> str:
        return self._location


def test_oss_location_applies_settings():
    """An oss:// table location enables virtual-hosted addressing and the oss->s3 alias."""
    props = {"s3.endpoint": "http://oss-cn-hangzhou.aliyuncs.com", "s3.access-key-id": "ak"}
    result = _convert_iceberg_file_io_properties_to_io_config(props, "oss://my-bucket/warehouse/db/table")
    assert result is not None
    assert result.s3.force_virtual_addressing is True
    assert result.protocol_aliases == {"oss": "s3"}
    # Table properties are still applied.
    assert result.s3.endpoint_url == "http://oss-cn-hangzhou.aliyuncs.com"
    assert result.s3.key_id == "ak"


def test_oss_location_no_props():
    """An oss:// table with no IO properties still gets the OSS settings.

    Covers credentials supplied via the environment rather than table properties.
    """
    result = _convert_iceberg_file_io_properties_to_io_config({}, "oss://my-bucket/warehouse/db/table")
    assert result is not None
    assert result.s3.force_virtual_addressing is True
    assert result.protocol_aliases == {"oss": "s3"}


def test_non_oss_location_with_props():
    """A non-oss:// location with properties leaves the OSS settings off."""
    props = {"s3.endpoint": "https://s3.us-west-2.amazonaws.com"}
    result = _convert_iceberg_file_io_properties_to_io_config(props, "s3://my-bucket/warehouse/db/table")
    assert result is not None
    assert result.s3.force_virtual_addressing is False
    assert result.protocol_aliases == {}
    assert result.s3.endpoint_url == "https://s3.us-west-2.amazonaws.com"


def test_non_oss_location_no_props_returns_none():
    """A non-oss:// location with no properties yields no IOConfig."""
    assert _convert_iceberg_file_io_properties_to_io_config({}, "s3://my-bucket/warehouse/db/table") is None


def test_no_location_with_props():
    """With no location (default), properties still convert and OSS settings stay off."""
    props = {"s3.endpoint": "https://s3.us-west-2.amazonaws.com"}
    result = _convert_iceberg_file_io_properties_to_io_config(props)
    assert result is not None
    assert result.s3.force_virtual_addressing is False
    assert result.protocol_aliases == {}
    assert result.s3.endpoint_url == "https://s3.us-west-2.amazonaws.com"


def test_no_location_no_props_returns_none():
    """With no location (default) and no properties, no IOConfig is produced."""
    assert _convert_iceberg_file_io_properties_to_io_config({}) is None


def test_resolve_explicit_io_config_wins():
    """An explicitly-provided IOConfig takes precedence over table props and the default."""
    explicit = IOConfig(s3=S3Config(region_name="us-east-1"))
    table = _FakeTable({"s3.region": "eu-west-1"}, "s3://bucket/warehouse/db/table")
    result = resolve_iceberg_io_config(table, explicit)
    assert result is explicit


def test_resolve_falls_back_to_table_properties():
    """With no explicit IOConfig, the table's FileIO properties are translated."""
    table = _FakeTable({"s3.endpoint": "https://s3.us-west-2.amazonaws.com"}, "s3://bucket/warehouse/db/table")
    result = resolve_iceberg_io_config(table, None)
    assert result is not None
    assert result.s3.endpoint_url == "https://s3.us-west-2.amazonaws.com"


def test_resolve_falls_back_to_default_io_config():
    """With no explicit IOConfig and no table props, the context default_io_config is used."""
    default = IOConfig(s3=S3Config(region_name="ap-south-1"))
    table = _FakeTable({}, "s3://bucket/warehouse/db/table")
    old = context.get_context().daft_planning_config.default_io_config
    context.set_planning_config(default_io_config=default)
    try:
        result = resolve_iceberg_io_config(table, None)
        assert result is not None
        assert result.s3.region_name == "ap-south-1"
    finally:
        context.set_planning_config(default_io_config=old)
