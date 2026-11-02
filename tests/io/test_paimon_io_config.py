from __future__ import annotations

from types import SimpleNamespace

import pytest

from daft.io.paimon._paimon import (
    _catalog_options_from_table,
    _convert_paimon_catalog_options_to_io_config,
    _options_to_dict,
)


class _ConfigOptionLike:
    """Stands in for ``pypaimon.common.options.ConfigOption``."""

    def __init__(self, key: str) -> None:
        self._key = key

    def key(self) -> str:
        return self._key


class _OptionsLike:
    """Stands in for ``pypaimon.common.options.Options`` (pypaimon >= 1.4).

    Its ``get()`` only accepts a ``ConfigOption``; passing a string raises the
    same ``AttributeError`` real pypaimon raises.
    """

    def __init__(self, data: dict) -> None:
        self.data = data

    def to_map(self) -> dict:
        return self.data

    def get(self, key, default=None):
        if not isinstance(key, _ConfigOptionLike):
            raise AttributeError(f"'{type(key).__name__}' object has no attribute 'key'")
        return self.data.get(key.key(), default)


class _CoreOptionsLike:
    """Stands in for ``pypaimon.common.options.CoreOptions`` (pypaimon >= 1.4)."""

    def __init__(self, options: _OptionsLike) -> None:
        self.options = options


def test_catalog_options_from_table_accepts_plain_dict():
    table = SimpleNamespace(file_io=SimpleNamespace(properties={"warehouse": "s3://bucket/warehouse"}))

    assert _catalog_options_from_table(table) == {"warehouse": "s3://bucket/warehouse"}


def test_catalog_options_from_table_accepts_pypaimon_options():
    properties = _OptionsLike({"warehouse": "s3://bucket/warehouse", "fs.s3.endpoint": "http://localhost:9000"})
    table = SimpleNamespace(file_io=SimpleNamespace(properties=properties))

    with pytest.raises(AttributeError):
        properties.get("warehouse")

    assert _catalog_options_from_table(table) == {
        "warehouse": "s3://bucket/warehouse",
        "fs.s3.endpoint": "http://localhost:9000",
    }


def test_catalog_options_from_table_without_file_io():
    assert _catalog_options_from_table(SimpleNamespace()) == {}


def test_options_to_dict_unwraps_core_options():
    assert _options_to_dict(_CoreOptionsLike(_OptionsLike({"file.format": "parquet"}))) == {"file.format": "parquet"}


def test_convert_paimon_s3_catalog_options_to_io_config():
    result = _convert_paimon_catalog_options_to_io_config(
        {
            "warehouse": "s3://my-bucket/warehouse",
            "fs.s3.endpoint": "http://localhost:9000",
            "fs.s3.region": "us-east-1",
            "fs.s3.accessKeyId": "minioadmin",
            "fs.s3.accessKeySecret": "minioadmin",
            "fs.s3.securityToken": "session-token",
        }
    )

    assert result is not None
    assert result.s3.endpoint_url == "http://localhost:9000"
    assert result.s3.region_name == "us-east-1"
    assert result.s3.key_id == "minioadmin"
    assert result.s3.access_key == "minioadmin"
    assert result.s3.session_token == "session-token"


def test_convert_paimon_oss_catalog_options_to_opendal_io_config():
    result = _convert_paimon_catalog_options_to_io_config(
        {
            "warehouse": "oss://my-bucket/warehouse",
            "fs.oss.endpoint": "oss-cn-hangzhou.aliyuncs.com",
            "fs.oss.region": "cn-hangzhou",
            "fs.oss.accessKeyId": "access-key-id",
            "fs.oss.accessKeySecret": "access-key-secret",
            "fs.oss.securityToken": "security-token",
        }
    )

    assert result is not None
    assert result.opendal_backends == {
        "oss": {
            "bucket": "my-bucket",
            "endpoint": "https://oss-cn-hangzhou.aliyuncs.com",
            "region": "cn-hangzhou",
            "access_key_id": "access-key-id",
            "access_key_secret": "access-key-secret",
            "security_token": "security-token",
        }
    }


def test_convert_paimon_catalog_options_without_storage_options_returns_none():
    assert _convert_paimon_catalog_options_to_io_config({"warehouse": "s3://my-bucket/warehouse"}) is None
