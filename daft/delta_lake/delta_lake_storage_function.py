from __future__ import annotations

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


def _storage_config_to_storage_options(storage_config: StorageConfig, table_uri: str) -> dict[str, str] | None:
    """
    Converts the Daft storage config to a storage options dict that deltalake/object_store
    understands.
    """
    config = storage_config.config
    assert isinstance(config, NativeStorageConfig)
    io_config = config.io_config
    return _io_config_to_storage_options(io_config, table_uri)


def _io_config_to_storage_options(io_config: IOConfig, table_uri: str) -> dict[str, str] | None:
    scheme = urlparse(table_uri).scheme
    if scheme == "s3" or scheme == "s3a":
        return _s3_config_to_storage_options(io_config.s3)
    elif scheme == "gcs" or scheme == "gs":
        return _gcs_config_to_storage_options(io_config.gcs)
    elif scheme == "az" or scheme == "abfs":
        return _azure_config_to_storage_options(io_config.azure)
    else:
        return None


def _s3_config_to_storage_options(s3_config: S3Config) -> dict[str, str]:
    storage_options: dict[str, Any] = {}
    if s3_config.region_name is not None:
        storage_options["region"] = s3_config.region_name
    if s3_config.endpoint_url is not None:
        storage_options["endpoint_url"] = s3_config.endpoint_url
    if s3_config.key_id is not None:
        storage_options["access_key_id"] = s3_config.key_id
    if s3_config.session_token is not None:
        storage_options["session_token"] = s3_config.session_token
    if s3_config.access_key is not None:
        storage_options["secret_access_key"] = s3_config.access_key
    if s3_config.use_ssl is not None:
        storage_options["allow_http"] = "false" if s3_config.use_ssl else "true"
    if s3_config.verify_ssl is not None:
        storage_options["allow_invalid_certificates"] = "false" if s3_config.verify_ssl else "true"
    if s3_config.connect_timeout_ms is not None:
        storage_options["connect_timeout"] = str(s3_config.connect_timeout_ms) + "ms"
    if s3_config.anonymous:
        raise ValueError(
            "Reading from DeltaLake does not support anonymous mode! Please supply credentials via your S3Config."
        )
    return storage_options


def _azure_config_to_storage_options(azure_config: AzureConfig) -> dict[str, str]:
    storage_options = {}
    if azure_config.storage_account is not None:
        storage_options["account_name"] = azure_config.storage_account
    if azure_config.access_key is not None:
        storage_options["access_key"] = azure_config.access_key
    if azure_config.endpoint_url is not None:
        storage_options["endpoint"] = azure_config.endpoint_url
    if azure_config.use_ssl is not None:
        storage_options["allow_http"] = "false" if azure_config.use_ssl else "true"
    return storage_options


def _gcs_config_to_storage_options(_: GCSConfig) -> dict[str, str]:
    return {}
