from __future__ import annotations

import warnings
from typing import Any
from urllib.parse import urlparse

from daft.daft import (
    AzureConfig,
    GCSConfig,
    IOConfig,
    NativeStorageConfig,
    S3Config,
    StorageConfig,
)


def _storage_config_to_storage_options(storage_config: StorageConfig, table_uri: str, **kwargs: Any) -> dict[str, str]:
    """
    Converts the Daft storage config to a storage options dict that deltalake/object_store
    understands.
    """
    config = storage_config.config
    assert isinstance(config, NativeStorageConfig)
    io_config = config.io_config
    return _io_config_to_storage_options(io_config, table_uri, **kwargs)


def _io_config_to_storage_options(io_config: IOConfig, table_uri: str, **kwargs: Any) -> dict[str, str]:
    scheme = urlparse(table_uri).scheme
    if scheme == "s3" or scheme == "s3a":
        return _s3_config_to_storage_options(io_config.s3, **kwargs)
    elif scheme == "gcs" or scheme == "gs":
        return _gcs_config_to_storage_options(io_config.gcs)
    elif scheme == "az" or scheme == "abfs":
        return _azure_config_to_storage_options(io_config.azure)
    else:
        return {}


def _s3_config_to_storage_options(s3_config: S3Config, **kwargs) -> dict[str, str]:
    storage_options: dict[str, Any] = {}

    dynamo_table_name = kwargs.get("dynamo_table_name")
    if dynamo_table_name is not None:
        storage_options["AWS_S3_LOCKING_PROVIDER"] = "dynamodb"
        storage_options["DELTA_DYNAMO_TABLE_NAME"] = dynamo_table_name
    else:
        storage_options["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
        warnings.warn("No DynamoDB table specified for Delta Lake locking. Defaulting to unsafe writes.")

    if s3_config.region_name is not None:
        storage_options["AWS_REGION"] = s3_config.region_name
    if s3_config.endpoint_url is not None:
        storage_options["AWS_ENDPOINT_URL"] = s3_config.endpoint_url
    if s3_config.key_id is not None:
        storage_options["AWS_ACCESS_KEY_ID"] = s3_config.key_id
    if s3_config.session_token is not None:
        storage_options["AWS_SESSION_TOKEN"] = s3_config.session_token
    if s3_config.access_key is not None:
        storage_options["AWS_SECRET_ACCESS_KEY"] = s3_config.access_key
    if s3_config.use_ssl is not None:
        storage_options["AWS_ALLOW_HTTP"] = "false" if s3_config.use_ssl else "true"
    # TODO: Find way to set this, might require upstream changes since the API is currently only exposed in Rust
    # if s3_config.verify_ssl is not None:
    #     storage_options["allow_invalid_certificates"] = "false" if s3_config.verify_ssl else "true"
    if s3_config.connect_timeout_ms is not None:
        storage_options["AWS_EC2_METADATA_TIMEOUT"] = str(s3_config.connect_timeout_ms)
    return storage_options


def _azure_config_to_storage_options(azure_config: AzureConfig) -> dict[str, str]:
    storage_options = {}
    if azure_config.storage_account is not None:
        storage_options["AZURE_STORAGE_ACCOUNT_NAME"] = azure_config.storage_account
    if azure_config.access_key is not None:
        storage_options["AZURE_STORAGE_ACCOUNT_KEY"] = azure_config.access_key
    return storage_options


def _gcs_config_to_storage_options(_: GCSConfig) -> dict[str, str]:
    # TODO: Support for GCS storage options
    return {}
