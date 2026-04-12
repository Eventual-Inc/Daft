# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

import posixpath
from typing import TYPE_CHECKING, Any, Union

from daft import context, runners
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder

if TYPE_CHECKING:
    from pyiceberg.table import Table as PyIcebergTable


def _resolve_metadata_location(location: str) -> str:
    """Resolve an Iceberg table location to a metadata file path.

    If the location already points to a ``.metadata.json`` file, it is returned
    as-is.  Otherwise, we treat it as a table root directory and attempt to read
    ``<location>/metadata/version-hint.text`` (the standard Iceberg version-hint
    file) to derive the metadata file path.

    This provides built-in support for ``version-hint.text`` regardless of which
    PyIceberg version is installed.
    """
    if location.endswith(".metadata.json"):
        return location

    from pyiceberg.io import load_file_io

    # Try version-hint.text (Iceberg spec standard)
    version_hint_path = posixpath.join(location, "metadata", "version-hint.text")
    io = load_file_io(properties={}, location=version_hint_path)
    try:
        input_file = io.new_input(version_hint_path)
        with input_file.open() as f:
            content = f.read().decode("utf-8").strip()
    except FileNotFoundError:
        # No version-hint file found; return the original location and let
        # pyiceberg handle it (it may raise its own error).
        return location

    if not content:
        return location
    if content.endswith(".metadata.json"):
        return posixpath.join(location, "metadata", content)
    elif content.isnumeric():
        return posixpath.join(location, "metadata", f"v{content}.metadata.json")
    else:
        return posixpath.join(location, "metadata", f"{content}.metadata.json")


def _convert_iceberg_file_io_properties_to_io_config(props: dict[str, Any]) -> IOConfig | None:
    """Property keys defined here: https://github.com/apache/iceberg-python/blob/main/pyiceberg/io/__init__.py."""
    from daft.io import AzureConfig, GCSConfig, IOConfig, S3Config

    any_props_set = False

    def get_first_property_value(*property_names: str) -> Any | None:
        for property_name in property_names:
            if property_value := props.get(property_name):
                nonlocal any_props_set
                any_props_set = True
                return property_value
        return None

    io_config = IOConfig(
        s3=S3Config(
            endpoint_url=get_first_property_value("s3.endpoint"),
            region_name=get_first_property_value("s3.region", "client.region"),
            key_id=get_first_property_value("s3.access-key-id", "client.access-key-id"),
            access_key=get_first_property_value("s3.secret-access-key", "client.secret-access-key"),
            session_token=get_first_property_value("s3.session-token", "client.session-token"),
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
    )

    return io_config if any_props_set else None


@PublicAPI
def read_iceberg(
    table: Union[str, "PyIcebergTable"],
    snapshot_id: int | None = None,
    io_config: IOConfig | None = None,
) -> DataFrame:
    """Create a DataFrame from an Iceberg table.

    Args:
        table (str or pyiceberg.table.Table): A path to an Iceberg metadata file, an Iceberg table location
            (directory), or a [PyIceberg Table](https://py.iceberg.apache.org/reference/pyiceberg/table/#pyiceberg.table.Table)
            created using the PyIceberg library. Supports remote URLs to object stores such as ``s3://`` or ``gs://``.
            When a table location is provided (i.e. a path that does not end in ``.metadata.json``), the metadata
            is resolved via ``<location>/metadata/version-hint.text``.
        snapshot_id (int, optional): Snapshot ID of the table to query
        io_config (IOConfig, optional): A custom IOConfig to use when accessing Iceberg object storage data. If provided, configurations set in `table` are ignored.

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

        Read an Iceberg table from a table location (uses version-hint.text):
        >>> df = daft.read_iceberg("/path/to/iceberg/table")
        >>> df.show()
    """
    from pyiceberg.table import StaticTable

    from daft.io.iceberg.iceberg_scan import IcebergScanOperator

    # support for read_iceberg('path/to/table') and read_iceberg('path/to/metadata.json')
    if isinstance(table, str):
        table = StaticTable.from_metadata(metadata_location=_resolve_metadata_location(table))

    io_config = (
        _convert_iceberg_file_io_properties_to_io_config(table.io.properties) if io_config is None else io_config
    )
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    multithreaded_io = runners.get_or_create_runner().name != "ray"
    storage_config = StorageConfig(multithreaded_io, io_config)

    iceberg_operator = IcebergScanOperator(table, snapshot_id=snapshot_id, storage_config=storage_config)

    handle = ScanOperatorHandle.from_python_scan_operator(iceberg_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)
