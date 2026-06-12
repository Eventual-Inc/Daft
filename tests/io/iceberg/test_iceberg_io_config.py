from __future__ import annotations

from daft.io.iceberg._iceberg import _convert_iceberg_file_io_properties_to_io_config


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


def test_schemeless_endpoint_gets_https_scheme():
    """A vended endpoint without a URI scheme is defaulted to https.

    Some catalogs vend `s3.endpoint` as a bare host; Daft's S3 client requires
    a full URI.
    """
    props = {"s3.endpoint": "s3.us-west-2.example.com"}
    result = _convert_iceberg_file_io_properties_to_io_config(props, "s3://my-bucket/warehouse/db/table")
    assert result is not None
    assert result.s3.endpoint_url == "https://s3.us-west-2.example.com"


def test_schemeless_endpoint_with_trailing_slash():
    """A bare-host endpoint with a trailing slash also gets the https scheme."""
    props = {"s3.endpoint": "s3.us-west-2.example.com/"}
    result = _convert_iceberg_file_io_properties_to_io_config(props, "s3://my-bucket/warehouse/db/table")
    assert result is not None
    assert result.s3.endpoint_url == "https://s3.us-west-2.example.com/"


def test_schemeless_endpoint_with_port():
    """A bare host:port endpoint also gets the https scheme."""
    props = {"s3.endpoint": "minio.example.com:9000"}
    result = _convert_iceberg_file_io_properties_to_io_config(props, "s3://my-bucket/warehouse/db/table")
    assert result is not None
    assert result.s3.endpoint_url == "https://minio.example.com:9000"


def test_endpoint_with_scheme_unchanged():
    """Endpoints that already carry a scheme are passed through untouched."""
    props = {"s3.endpoint": "http://minio.example.com:9000"}
    result = _convert_iceberg_file_io_properties_to_io_config(props, "s3://my-bucket/warehouse/db/table")
    assert result is not None
    assert result.s3.endpoint_url == "http://minio.example.com:9000"
