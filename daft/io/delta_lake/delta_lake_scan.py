from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from urllib.error import HTTPError
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen

import deltalake
from deltalake.table import DeltaTable
from packaging.version import parse

import daft
import daft.exceptions
from daft.daft import (
    FileFormatConfig,
    ParquetSourceConfig,
    PyPartitionField,
    PyPushdowns,
    S3Config,
    ScanTask,
    StorageConfig,
)
from daft.io.delta_lake.utils import construct_delta_file_path
from daft.io.object_store_options import io_config_to_storage_options
from daft.io.scan import ScanOperator
from daft.logical.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime

logger = logging.getLogger(__name__)


def get_s3_bucket_region(bucket_name: str) -> str | None:
    # When making a https request to https://{bucket_name}.s3.amazonaws.com/, aws returns either a 200 response, 403 response, or 404 response.
    # In the header of the 200 and 403 responses, there is an `x-amz-bucket-region` field from which we can extract the bucket's region.
    url = f"https://{bucket_name}.s3.amazonaws.com"

    try:
        req = Request(url, method="HEAD")
        with urlopen(req) as response:
            return response.headers.get("x-amz-bucket-region")
    except HTTPError as e:
        bucket_region = e.headers.get("x-amz-bucket-region")
        if bucket_region is None:
            logger.warning(
                "Failed to get the S3 bucket region using the given S3 uri. HTTPError error: %s",
                e,
            )
        return bucket_region
    except Exception as e:
        logger.warning(
            "Failed to get the S3 bucket region using the given S3 uri. Error: %s",
            e,
        )
        return None


class DeltaLakeScanOperator(ScanOperator):
    def __init__(
        self,
        table_uri: str,
        storage_config: StorageConfig,
        version: int | str | datetime | None = None,
        ignore_deletion_vectors: bool = False,
    ) -> None:
        super().__init__()

        # Unfortunately delta-rs doesn't do very good inference of credentials for S3. Thus the current Daft behavior of passing
        # in `None` for credentials will cause issues when instantiating the DeltaTable without credentials.
        #
        # Thus, if we don't detect any credentials being available, we attempt to detect it from the environment using our Daft credentials chain.
        #
        # See: https://github.com/delta-io/delta-rs/issues/2117
        deltalake_sdk_io_config = storage_config.io_config
        scheme = urlparse(table_uri).scheme
        if scheme == "s3" or scheme == "s3a":
            # Try to get the bucket's region.
            if deltalake_sdk_io_config.s3.region_name is None:
                bucket_name = urlparse(table_uri).netloc
                region = get_s3_bucket_region(bucket_name)
                if region is not None:
                    deltalake_sdk_io_config = deltalake_sdk_io_config.replace(
                        s3=deltalake_sdk_io_config.s3.replace(region_name=region)
                    )

            # Try to get config from the environment
            if any(
                [
                    deltalake_sdk_io_config.s3.key_id is None,
                    deltalake_sdk_io_config.s3.region_name is None,
                ]
            ):
                try:
                    s3_config_from_env = S3Config.from_env()
                # Sometimes S3Config.from_env throws an error, for example on CI machines with weird metadata servers.
                except daft.exceptions.DaftCoreException:
                    pass
                else:
                    if (
                        deltalake_sdk_io_config.s3.key_id is None
                        and deltalake_sdk_io_config.s3.access_key is None
                        and deltalake_sdk_io_config.s3.session_token is None
                    ):
                        deltalake_sdk_io_config = deltalake_sdk_io_config.replace(
                            s3=deltalake_sdk_io_config.s3.replace(
                                key_id=s3_config_from_env.key_id,
                                access_key=s3_config_from_env.access_key,
                                session_token=s3_config_from_env.session_token,
                            )
                        )
                    if deltalake_sdk_io_config.s3.region_name is None:
                        deltalake_sdk_io_config = deltalake_sdk_io_config.replace(
                            s3=deltalake_sdk_io_config.s3.replace(
                                region_name=s3_config_from_env.region_name,
                            )
                        )
        elif scheme == "gcs" or scheme == "gs":
            # TO-DO: Handle any key-value replacements in `io_config` if there are missing elements
            pass
        elif scheme == "az" or scheme == "abfs" or scheme == "abfss":
            # TO-DO: Handle any key-value replacements in `io_config` if there are missing elements
            pass

        self._table = DeltaTable(
            table_uri,
            storage_options=io_config_to_storage_options(deltalake_sdk_io_config, table_uri),
        )

        if version is not None:
            self._table.load_as_version(version)

        self._storage_config = storage_config

        from ._deltalake import delta_schema_to_pyarrow

        self._schema = Schema.from_pyarrow_schema(delta_schema_to_pyarrow(self._table.schema()))
        partition_columns = set(self._table.metadata().partition_columns)
        self._partition_keys = [
            PyPartitionField(field._field) for field in self._schema if field.name in partition_columns
        ]
        self._ignore_deletion_vectors = ignore_deletion_vectors

    def schema(self) -> Schema:
        return self._schema

    def name(self) -> str:
        return "DeltaLakeScanOperator"

    def display_name(self) -> str:
        return f"DeltaLakeScanOperator({self._table.metadata().name})"

    def partitioning_keys(self) -> list[PyPartitionField]:
        return self._partition_keys

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
            f"Partitioning keys = {self.partitioning_keys()}",
            # TODO(Clark): Improve repr of storage config here.
            f"Storage config = {self._storage_config}",
        ]

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        import pyarrow as pa

        # TODO(Clark): Push limit and filter expressions into deltalake action fetch, to prune the files returned.
        # Issue: https://github.com/Eventual-Inc/Daft/issues/1953
        add_actions = pa.record_batch(self._table.get_add_actions())
        if len(self.partitioning_keys()) > 0 and pushdowns.partition_filters is None:
            logger.warning(
                "%s has partitioning keys = %s, but no partition filter was specified. This will result in a full table scan.",
                self.display_name(),
                self.partitioning_keys(),
            )

        # TODO(Clark): Add support for deletion vectors.
        # Issue: https://github.com/Eventual-Inc/Daft/issues/1954
        if not self._ignore_deletion_vectors and "deletionVector" in add_actions.schema.names:
            raise NotImplementedError(
                "Delta Lake deletion vectors are not yet supported; please let the Daft team know if you'd like to see this feature!\n"
                "Deletion records can be dropped from this table to allow it to be read with Daft: https://docs.delta.io/latest/delta-drop-feature.html\n"
                "Alternatively, you can set ignore_deletion_vectors=True to skip checking for deletion vectors."
            )

        # TODO(Clark): Add support for column mappings.
        # Issue: https://github.com/Eventual-Inc/Daft/issues/1955

        limit_files = pushdowns.limit is not None and pushdowns.filters is None and pushdowns.partition_filters is None
        rows_left = pushdowns.limit if pushdowns.limit is not None else 0
        scan_tasks = []

        # Determine which partition field name is used in the schema
        # Deltalake versions <1.2.0 use "partition_values", >=1.2.0 use "partition"
        is_deltalake_v1_2_or_above = parse(deltalake.__version__) >= parse("1.2.0")

        partition_field_name = "partition" if is_deltalake_v1_2_or_above else "partition_values"
        is_partitioned = (
            partition_field_name in add_actions.schema.names
            and add_actions.schema.field(partition_field_name).type.num_fields > 0
        )
        for task_idx in range(add_actions.num_rows):
            if limit_files and rows_left <= 0:
                break

            # NOTE: The paths in the transaction log consist of the post-table-uri suffix.
            # Workaround for deltalake >= 1.2.0: paths are double-encoded in the log but single-encoded on disk.
            scheme = urlparse(self._table.table_uri).scheme
            raw_path = add_actions["path"][task_idx].as_py()
            if is_deltalake_v1_2_or_above:
                # For Delta Lake >= 1.2.0, decode double-encoded paths
                unquoted = unquote(raw_path)
                decoded_path = unquoted if unquoted != raw_path else raw_path
            else:
                # For Delta Lake < 1.2.0, use path as-is
                decoded_path = raw_path
            path = construct_delta_file_path(scheme, self._table.table_uri, decoded_path)

            try:
                record_count = add_actions["num_records"][task_idx].as_py()
            except KeyError:
                record_count = None

            try:
                size_bytes = add_actions["size_bytes"][task_idx].as_py()
            except KeyError:
                size_bytes = None
            file_format_config = FileFormatConfig.from_parquet_config(ParquetSourceConfig())

            if is_partitioned:
                dtype = add_actions.schema.field(partition_field_name).type
                part_values = add_actions[partition_field_name][task_idx]
                arrays = {}
                for field_idx in range(dtype.num_fields):
                    field_name = dtype.field(field_idx).name
                    try:
                        arrow_arr = pa.array([part_values[field_name]], type=dtype.field(field_idx).type)
                    except (
                        pa.ArrowInvalid,
                        pa.ArrowTypeError,
                        pa.ArrowNotImplementedError,
                    ):
                        # pyarrow < 13.0.0 doesn't accept pyarrow scalars in the array constructor.
                        arrow_arr = pa.array(
                            [part_values[field_name].as_py()],
                            type=dtype.field(field_idx).type,
                        )
                    arrays[field_name] = daft.Series.from_arrow(arrow_arr, field_name)
                partition_values = daft.recordbatch.RecordBatch.from_pydict(arrays)._recordbatch
            else:
                partition_values = None

            # Populate scan task with column-wise stats.
            schema_names = add_actions.schema.names
            if "min" in schema_names and "max" in schema_names:
                dtype = add_actions.schema.field("min").type
                min_values = add_actions["min"][task_idx]
                max_values = add_actions["max"][task_idx]
                # TODO(Clark): Add support for tracking null counts in column stats.
                # null_counts = add_actions["null_count"][task_idx]
                arrays = {}
                for field_idx in range(dtype.num_fields):
                    field_name = dtype.field(field_idx).name
                    try:
                        arrow_arr = pa.array(
                            [min_values[field_name], max_values[field_name]],
                            type=dtype.field(field_idx).type,
                        )
                    except (
                        pa.ArrowInvalid,
                        pa.ArrowTypeError,
                        pa.ArrowNotImplementedError,
                    ):
                        # pyarrow < 13.0.0 doesn't accept pyarrow scalars in the array constructor.
                        arrow_arr = pa.array(
                            [
                                min_values[field_name].as_py(),
                                max_values[field_name].as_py(),
                            ],
                            type=dtype.field(field_idx).type,
                        )
                    arrays[field_name] = daft.Series.from_arrow(arrow_arr, field_name)
                stats = daft.recordbatch.RecordBatch.from_pydict(arrays)
            else:
                stats = None
            st = ScanTask.catalog_scan_task(
                file=path,
                file_format=file_format_config,
                schema=self._schema._schema,
                num_rows=record_count,
                storage_config=self._storage_config,
                size_bytes=size_bytes,
                iceberg_delete_files=None,
                pushdowns=pushdowns,
                partition_values=partition_values,
                stats=stats._recordbatch if stats is not None else None,
            )
            if st is None:
                continue
            if record_count is not None:
                rows_left -= record_count
            scan_tasks.append(st)
        return iter(scan_tasks)

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return True
