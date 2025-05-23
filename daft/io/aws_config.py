from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import boto3

    from daft.daft import S3Config


def boto3_client_from_s3_config(service: str, s3_config: S3Config) -> boto3.client:
    import boto3

    return boto3.client(
        service,
        region_name=s3_config.region_name,
        use_ssl=s3_config.use_ssl,
        verify=s3_config.verify_ssl,
        endpoint_url=s3_config.endpoint_url,
        aws_access_key_id=s3_config.key_id,
        aws_secret_access_key=s3_config.access_key,
        aws_session_token=s3_config.session_token,
    )
