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
    table_uri = "s3://dummy_bucket/path"
    config = io_config_to_storage_options(IOConfig(s3=s3_config), table_uri=table_uri)
    assert config == {
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

    s3_config = s3_config.replace(force_virtual_addressing=False)
    config = io_config_to_storage_options(IOConfig(s3=s3_config), table_uri=table_uri)
    assert config == {
        "region": "us-east-2",
        "endpoint_url": "https://s3.us-east-2.amazonaws.com",
        "access_key_id": "dummy_ak",
        "secret_access_key": "dummy_sk",
        "allow_http": "false",
        "allow_invalid_certificates": "false",
        "connect_timeout": "1000ms",
        "skip_signature": "true",
    }
