# isort: dont-add-import: from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, NativeStorageConfig, ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder

if TYPE_CHECKING:
    from pyiceberg.table import Table as PyIcebergTable


def _convert_iceberg_file_io_properties_to_io_config(props: Dict[str, Any]) -> Optional["IOConfig"]:
    import pyiceberg
    from packaging.version import parse
    from pyiceberg.io import (
        S3_ACCESS_KEY_ID,
        S3_ENDPOINT,
        S3_REGION,
        S3_SECRET_ACCESS_KEY,
        S3_SESSION_TOKEN,
    )

    from daft.io import AzureConfig, GCSConfig, IOConfig, S3Config

    s3_mapping = {
        S3_REGION: "region_name",
        S3_ENDPOINT: "endpoint_url",
        S3_ACCESS_KEY_ID: "key_id",
        S3_SECRET_ACCESS_KEY: "access_key",
        S3_SESSION_TOKEN: "session_token",
    }
    s3_args = dict()  # type: ignore
    for pyiceberg_key, daft_key in s3_mapping.items():
        value = props.get(pyiceberg_key, None)
        if value is not None:
            s3_args[daft_key] = value

    if len(s3_args) > 0:
        s3_config = S3Config(**s3_args)
    else:
        s3_config = None

    gcs_config = None
    azure_config = None
    if parse(pyiceberg.__version__) >= parse("0.5.0"):
        from pyiceberg.io import GCS_PROJECT_ID, GCS_TOKEN

        gcs_mapping = {GCS_PROJECT_ID: "project_id", GCS_TOKEN: "token"}
        gcs_args = dict()  # type: ignore
        for pyiceberg_key, daft_key in gcs_mapping.items():
            value = props.get(pyiceberg_key, None)
            if value is not None:
                gcs_args[daft_key] = value

        if len(gcs_args) > 0:
            gcs_config = GCSConfig(**gcs_args)

        azure_mapping = {
            "adlfs.account-name": "storage_account",
            "adlfs.account-key": "access_key",
            "adlfs.sas-token": "sas_token",
            "adlfs.tenant-id": "tenant_id",
            "adlfs.client-id": "client_id",
            "adlfs.client-secret": "client_secret",
        }

        azure_args = dict()  # type: ignore
        for pyiceberg_key, daft_key in azure_mapping.items():
            value = props.get(pyiceberg_key, None)
            if value is not None:
                azure_args[daft_key] = value

        if len(azure_args) > 0:
            azure_config = AzureConfig(**azure_args)

    if any([s3_config, gcs_config, azure_config]):
        return IOConfig(s3=s3_config, gcs=gcs_config, azure=azure_config)
    else:
        return None


@PublicAPI
def read_iceberg(
    pyiceberg_table: "PyIcebergTable",
    snapshot_id: Optional[int] = None,
    io_config: Optional["IOConfig"] = None,
) -> DataFrame:
    """Create a DataFrame from an Iceberg table

    Example:
        >>> import pyiceberg
        >>>
        >>> pyiceberg_table = pyiceberg.Table(...)
        >>> df = daft.read_iceberg(pyiceberg_table)
        >>>
        >>> # Filters on this dataframe can now be pushed into
        >>> # the read operation from Iceberg
        >>> df = df.where(df["foo"] > 5)
        >>> df.show()

    .. NOTE::
        This function requires the use of `PyIceberg <https://py.iceberg.apache.org/>`_, which is the Apache Iceberg's
        official project for Python.

    Args:
        pyiceberg_table: Iceberg table created using the PyIceberg library
        snapshot_id: Snapshot ID of the table to query
        io_config: A custom IOConfig to use when accessing Iceberg object storage data. Defaults to None.

    Returns:
        DataFrame: a DataFrame with the schema converted from the specified Iceberg table
    """
    from daft.iceberg.iceberg_scan import IcebergScanOperator

    io_config = (
        _convert_iceberg_file_io_properties_to_io_config(pyiceberg_table.io.properties)
        if io_config is None
        else io_config
    )
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    multithreaded_io = not context.get_context().is_ray_runner
    storage_config = StorageConfig.native(NativeStorageConfig(multithreaded_io, io_config))

    iceberg_operator = IcebergScanOperator(pyiceberg_table, snapshot_id=snapshot_id, storage_config=storage_config)

    handle = ScanOperatorHandle.from_python_scan_operator(iceberg_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)
