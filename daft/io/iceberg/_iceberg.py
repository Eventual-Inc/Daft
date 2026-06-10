# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any, Union

from daft import context, runners
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.filesystem import get_protocol_from_path
from daft.io._checkpoint import attach_checkpoint
from daft.logical.builder import LogicalPlanBuilder

if TYPE_CHECKING:
    from pyiceberg.table import Table as PyIcebergTable

    from daft.checkpoint import CheckpointConfig


logger = logging.getLogger(__name__)


def _convert_iceberg_file_io_properties_to_io_config(
    props: dict[str, Any], location: str | None = None
) -> IOConfig | None:
    """Property keys defined here: https://github.com/apache/iceberg-python/blob/main/pyiceberg/io/__init__.py.

    For an ``oss://`` ``location`` (Alibaba Cloud OSS, S3-compatible), the IOConfig gets
    virtual-hosted addressing and an ``oss``->``s3`` alias so the S3 filesystem resolves
    ``oss://`` paths -- applied even with no IO properties (e.g. env-var credentials).
    """
    from daft.io import AzureConfig, GCSConfig, IOConfig, S3Config

    any_props_set = False

    def get_first_property_value(*property_names: str) -> Any | None:
        for property_name in property_names:
            if property_value := props.get(property_name):
                nonlocal any_props_set
                any_props_set = True
                return property_value
        return None

    is_oss = location is not None and get_protocol_from_path(location) == "oss"

    io_config = IOConfig(
        s3=S3Config(
            endpoint_url=get_first_property_value("s3.endpoint"),
            region_name=get_first_property_value("s3.region", "client.region"),
            key_id=get_first_property_value("s3.access-key-id", "client.access-key-id"),
            access_key=get_first_property_value("s3.secret-access-key", "client.secret-access-key"),
            session_token=get_first_property_value("s3.session-token", "client.session-token"),
            force_virtual_addressing=True if is_oss else None,
        ),
        azure=AzureConfig(
            storage_account=get_first_property_value("adls.account-name", "adlfs.account-name"),
            access_key=get_first_property_value("adls.account-key", "adlfs.account-key"),
            sas_token=get_first_property_value("adls.sas-token", "adlfs.sas-token"),
            tenant_id=get_first_property_value("adls.tenant-id", "adlfs.tenant-id"),
            client_id=get_first_property_value("adls.client-id", "adlfs.client-id"),
            client_secret=get_first_property_value("adls.client-secret", "adlfs.client-secret"),
        ),
        gcs=GCSConfig(
            project_id=get_first_property_value("gcs.project-id"),
            token=get_first_property_value("gcs.oauth2.token"),
        ),
        protocol_aliases={"oss": "s3"} if is_oss else None,
    )

    if is_oss:
        logger.debug("oss:// table detected; applying S3-compatible settings to the IOConfig")
        return io_config
    return io_config if any_props_set else None


def _resolve_ref_snapshot_id(table: "PyIcebergTable", ref_name: str, ref_kind: str) -> int:
    from pyiceberg.table.refs import SnapshotRefType  # pyiceberg is an optional dependency

    expected_ref_type = SnapshotRefType.BRANCH if ref_kind == "branch" else SnapshotRefType.TAG
    ref = table.refs().get(ref_name)

    if ref is None:
        raise ValueError(f"Iceberg {ref_kind} {ref_name!r} does not exist")

    if ref.snapshot_ref_type != expected_ref_type:
        raise ValueError(f"Iceberg {ref_kind} {ref_name!r} is a {ref.snapshot_ref_type.value}")

    return ref.snapshot_id


def _resolve_snapshot_id(
    table: "PyIcebergTable",
    snapshot_id: int | None,
    branch: str | None,
    tag: str | None,
) -> int | None:
    if sum(value is not None for value in (snapshot_id, branch, tag)) > 1:
        raise ValueError("Only one of snapshot_id, branch, or tag may be provided")

    if branch is not None:
        return _resolve_ref_snapshot_id(table, branch, "branch")
    if tag is not None:
        return _resolve_ref_snapshot_id(table, tag, "tag")

    return snapshot_id


@PublicAPI
def read_iceberg(
    table: Union[str, os.PathLike[str], "PyIcebergTable"],
    snapshot_id: int | None = None,
    branch: str | None = None,
    tag: str | None = None,
    io_config: IOConfig | None = None,
    checkpoint: "CheckpointConfig | None" = None,
) -> DataFrame:
    """Create a DataFrame from an Iceberg table.

    Args:
        table (str, os.PathLike, or pyiceberg.table.Table): A path to an Iceberg metadata file (supports remote URLs
            to object stores such as ``s3://`` or ``gs://``) or a
            [PyIceberg Table](https://py.iceberg.apache.org/reference/pyiceberg/table/#pyiceberg.table.Table) created
            using the PyIceberg library.
        snapshot_id (int, optional): Snapshot ID of the table to query
        branch (str, optional): Iceberg branch name to query. Cannot be combined with ``snapshot_id`` or ``tag``.
        tag (str, optional): Iceberg tag name to query. Cannot be combined with ``snapshot_id`` or ``branch``.
        io_config (IOConfig, optional): A custom IOConfig to use when accessing Iceberg object storage data. If provided, configurations set in `table` are ignored.
        checkpoint: Optional :class:`daft.CheckpointConfig` for progress tracking across runs. Bundles the
            checkpoint store, the source key column (``on=``), and optional anti-join tuning. Rows whose key
            already exists in the store are skipped on re-run. Requires the Ray runner.

    Returns:
        DataFrame: a DataFrame with the schema converted from the specified Iceberg table

    Note:
        This function requires the use of [PyIceberg](https://py.iceberg.apache.org/), which is the Apache Iceberg's
        official project for Python.

    Examples:
        Read an Iceberg table from a PyIceberg table:
        >>> import pyiceberg
        >>>
        >>> table = pyiceberg.Table(...)
        >>> df = daft.read_iceberg(table)
        >>>
        >>> # Filters on this dataframe can now be pushed into the read operation from Iceberg
        >>> df = df.where(df["foo"] > 5)
        >>> df.show()

        Read an Iceberg table from S3 using IOConfig:
        >>> from daft.io import S3Config, IOConfig
        >>> io_config = IOConfig(s3=S3Config(region="us-west-2", anonymous=True))
        >>> df = daft.read_iceberg("s3://bucket/path/to/iceberg/metadata.json", io_config=io_config)
        >>> df.show()
    """
    from pyiceberg.table import StaticTable

    from daft.io.iceberg.iceberg_scan import IcebergScanOperator

    # support for read_iceberg('path/to/metadata.json')
    if isinstance(table, (str, os.PathLike)):
        table = StaticTable.from_metadata(metadata_location=os.fspath(table))

    snapshot_id = _resolve_snapshot_id(table, snapshot_id, branch, tag)

    io_config = (
        _convert_iceberg_file_io_properties_to_io_config(table.io.properties, table.location())
        if io_config is None
        else io_config
    )
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    multithreaded_io = runners.get_or_create_runner().name != "ray"
    storage_config = StorageConfig(multithreaded_io, io_config)

    iceberg_operator = IcebergScanOperator(table, snapshot_id=snapshot_id, storage_config=storage_config)

    handle = ScanOperatorHandle.from_python_scan_operator(iceberg_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    builder = attach_checkpoint(builder, checkpoint)
    return DataFrame(builder)
