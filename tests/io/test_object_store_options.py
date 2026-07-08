from __future__ import annotations

from daft.daft import IOConfig, S3Config
from daft.io.object_store_options import io_config_to_storage_options


def test_convert_to_s3_config():
    s3_config = S3Config(
        region_name="us-east-2",
        endpoint_url="https://s3.us-east-2.amazonaws.com",
        key_id="dummy_ak",
        access_key="dummy_sk",
        use_ssl=True,
        verify_ssl=True,
        connect_timeout_ms=1000,
        anonymous=True,
        force_virtual_addressing=True,
    )
    expected_s3_config = {
        "region": "us-east-2",
        "endpoint_url": "https://dummy_bucket.s3.us-east-2.amazonaws.com",
        "access_key_id": "dummy_ak",
        "secret_access_key": "dummy_sk",
        "allow_http": "false",
        "allow_invalid_certificates": "false",
        "connect_timeout": "1000ms",
        "skip_signature": "true",
        "virtual_hosted_style_request": "true",
    }
    table_uri = "s3://dummy_bucket/path"

    # convert when endpoint doesn't contain bucket for virtual host style
    config = io_config_to_storage_options(IOConfig(s3=s3_config), table_uri=table_uri)
    assert config == expected_s3_config

    # convert when endpoint contains bucket for virtual host style
    new_s3_config = s3_config.replace(endpoint_url="https://dummy_bucket.s3.us-east-2.amazonaws.com")
    config = io_config_to_storage_options(IOConfig(s3=new_s3_config), table_uri=table_uri)
    assert config == expected_s3_config

    # convert when force virtual addressing is false
    new_s3_config = s3_config.replace(force_virtual_addressing=False)
    config = io_config_to_storage_options(IOConfig(s3=new_s3_config), table_uri=table_uri)
    expected_s3_config.update(endpoint_url="https://s3.us-east-2.amazonaws.com")
    expected_s3_config.pop("virtual_hosted_style_request")
    assert config == expected_s3_config


def test_convert_oss_to_s3_config():
    # Alibaba Cloud OSS is S3-compatible and is routed through the S3 storage options.
    # See: https://github.com/Eventual-Inc/Daft/issues/5539
    s3_config = S3Config(
        region_name="oss-cn-hangzhou",
        endpoint_url="https://oss-cn-hangzhou.aliyuncs.com",
        key_id="dummy_ak",
        access_key="dummy_sk",
        force_virtual_addressing=True,
    )
    table_uri = "oss://dummy_bucket/path"

    config = io_config_to_storage_options(IOConfig(s3=s3_config), table_uri=table_uri)

    # OSS must resolve to the S3 storage options (previously returned None).
    assert config is not None
    assert config["region"] == "oss-cn-hangzhou"
    assert config["access_key_id"] == "dummy_ak"
    assert config["secret_access_key"] == "dummy_sk"
    # Virtual-hosted-style addressing rewrites the endpoint to include the bucket.
    assert config["virtual_hosted_style_request"] == "true"
    assert config["endpoint_url"] == "https://dummy_bucket.oss-cn-hangzhou.aliyuncs.com"
