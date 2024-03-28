from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlparse

import daft
from daft.daft import (
    AzureConfig,
    FileFormatConfig,
    GCSConfig,
    IOConfig,
    NativeStorageConfig,
    ParquetSourceConfig,
    Pushdowns,
    S3Config,
    ScanTask,
    StorageConfig,
)
from daft.hudi.pyhudi.table import HudiTable, HudiTableMetadata
from daft.io.scan import PartitionField, ScanOperator
from daft.logical.schema import Schema

logger = logging.getLogger(__name__)


class HudiScanOperator(ScanOperator):
    def __init__(self, table_uri: str, storage_config: StorageConfig) -> None:
        super().__init__()
        storage_options = _storage_config_to_storage_options(storage_config, table_uri)
        self._table = HudiTable(table_uri, storage_options=storage_options)
        self._storage_config = storage_config
        self._schema = Schema.from_pyarrow_schema(self._table.schema)
        partition_fields = set(self._table.props.partition_fields)
        self._partition_keys = [
            PartitionField(field._field) for field in self._schema if field.name in partition_fields
        ]

    def schema(self) -> Schema:
        return self._schema

    def display_name(self) -> str:
        return f"HudiScanOperator({self._table.props.name})"

    def partitioning_keys(self) -> list[PartitionField]:
        return self._partition_keys

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
            f"Partitioning keys = {self.partitioning_keys()}",
            f"Storage config = {self._storage_config}",
        ]

    def to_scan_tasks(self, pushdowns: Pushdowns) -> Iterator[ScanTask]:
        import pyarrow as pa

        hudi_table_metadata: HudiTableMetadata = self._table.latest_table_metadata()
        files_metadata = hudi_table_metadata.files_metadata

        if len(self.partitioning_keys()) > 0 and pushdowns.partition_filters is None:
            logging.warning(
                f"{self.display_name()} has partitioning keys = {self.partitioning_keys()}, but no partition filter was specified. This will result in a full table scan."
            )

        limit_files = pushdowns.limit is not None and pushdowns.filters is None and pushdowns.partition_filters is None
        rows_left = pushdowns.limit if pushdowns.limit is not None else 0
        scan_tasks = []
        for task_idx in range(files_metadata.num_rows):
            if limit_files and rows_left <= 0:
                break

            path = files_metadata["path"][task_idx].as_py()
            record_count = files_metadata["num_records"][task_idx].as_py()
            try:
                size_bytes = files_metadata["size_bytes"][task_idx].as_py()
            except KeyError:
                size_bytes = None
            file_format_config = FileFormatConfig.from_parquet_config(ParquetSourceConfig())

            if self._table.is_partitioned:
                dtype = files_metadata.schema.field("partition_values").type
                part_values = files_metadata["partition_values"][task_idx]
                arrays = {}
                for field_idx in range(dtype.num_fields):
                    field_name = dtype.field(field_idx).name
                    try:
                        arrow_arr = pa.array([part_values[field_name]], type=dtype.field(field_idx).type)
                    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
                        # pyarrow < 13.0.0 doesn't accept pyarrow scalars in the array constructor.
                        arrow_arr = pa.array([part_values[field_name].as_py()], type=dtype.field(field_idx).type)
                    arrays[field_name] = daft.Series.from_arrow(arrow_arr, field_name)
                partition_values = daft.table.Table.from_pydict(arrays)._table
            else:
                partition_values = None

            # Populate scan task with column-wise stats.
            schema = self._table.schema
            min_values = hudi_table_metadata.colstats_min_values
            max_values = hudi_table_metadata.colstats_max_values
            arrays = {}
            for field_idx in range(len(schema)):
                field_name = schema.field(field_idx).name
                field_type = schema.field(field_idx).type
                try:
                    arrow_arr = pa.array(
                        [min_values[field_name][task_idx], max_values[field_name][task_idx]], type=field_type
                    )
                except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
                    # pyarrow < 13.0.0 doesn't accept pyarrow scalars in the array constructor.
                    arrow_arr = pa.array(
                        [min_values[field_name][task_idx].as_py(), max_values[field_name][task_idx].as_py()],
                        type=field_type,
                    )
                arrays[field_name] = daft.Series.from_arrow(arrow_arr, field_name)
            stats = daft.table.Table.from_pydict(arrays)._table

            st = ScanTask.catalog_scan_task(
                file=path,
                file_format=file_format_config,
                schema=self._schema._schema,
                num_rows=record_count,
                storage_config=self._storage_config,
                size_bytes=size_bytes,
                pushdowns=pushdowns,
                partition_values=partition_values,
                stats=stats,
            )
            if st is None:
                continue
            rows_left -= record_count
            scan_tasks.append(st)
        return iter(scan_tasks)

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return True


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
    return storage_options


def _azure_config_to_storage_options(azure_config: AzureConfig) -> dict[str, str]:
    storage_options = {}
    if azure_config.storage_account is not None:
        storage_options["account_name"] = azure_config.storage_account
    if azure_config.access_key is not None:
        storage_options["access_key"] = azure_config.access_key
    return storage_options


def _gcs_config_to_storage_options(_: GCSConfig) -> dict[str, str]:
    return {}
