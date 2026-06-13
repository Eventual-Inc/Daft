from __future__ import annotations

from daft.io.paimon._paimon import _convert_paimon_catalog_options_to_io_config


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
