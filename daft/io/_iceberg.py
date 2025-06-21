# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Union

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder

if TYPE_CHECKING:
    from pyiceberg.table import Table as PyIcebergTable


def _convert_iceberg_file_io_properties_to_io_config(props: dict[str, Any]) -> Optional[IOConfig]:
    """Property keys defined here: https://github.com/apache/iceberg-python/blob/main/pyiceberg/io/__init__.py."""
    from daft.io import AzureConfig, GCSConfig, IOConfig, S3Config

    any_props_set = False

    def get_first_property_value(*property_names: str) -> Optional[Any]:
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


def _is_path_identifier(table: str) -> bool:
    return "/" in table and table.endswith(".json")


@PublicAPI
def read_iceberg(
    table: Union[str, "PyIcebergTable"],
    snapshot_id: Optional[int] = None,
    io_config: Optional[IOConfig] = None,
    iceberg_catalog_config: Optional[dict[str, str]] = None,
) -> DataFrame:
    """Create a DataFrame from an Iceberg table.

    Args:
        table (str or pyiceberg.table.Table): [PyIceberg Table](https://py.iceberg.apache.org/reference/pyiceberg/table/#pyiceberg.table.Table) created using the PyIceberg library or table identifier such as 'catalog.db.table'
        snapshot_id (int, optional): Snapshot ID of the table to query
        io_config (IOConfig, optional): A custom IOConfig to use when accessing Iceberg object storage data. If provided, configurations set in `table` are ignored.
        iceberg_catalog_config(dict[str, str], optional): Catalog configuration used when loading an Iceberg catalog if the `table` parameter is provided as a table identifier.

    Returns:
        DataFrame: a DataFrame with the schema converted from the specified Iceberg table

    Note:
        This function requires the use of [PyIceberg](https://py.iceberg.apache.org/), which is the Apache Iceberg's
        official project for Python.

    Examples:
        >>> import pyiceberg
        >>>
        >>> table = pyiceberg.Table(...)
        >>> df = daft.read_iceberg(table)
        >>>
        >>> # Filters on this dataframe can now be pushed into
        >>> # the read operation from Iceberg
        >>> df = df.where(df["foo"] > 5)
        >>> df.show()
        >>>
        >>> df2 = daft.read_iceberg(
        ...     "default.db.table",
        ...     iceberg_catalog_config={"type": "rest", "uri": "http://rest:8181", "s3.endpoint": "http://xxxx:port"},
        ... )

    """
    from pyiceberg.table import StaticTable

    from daft.iceberg.iceberg_scan import IcebergScanOperator

    # support for read_iceberg('path/to/metadata.json')
    if isinstance(table, str):
        if _is_path_identifier(table):
            table = StaticTable.from_metadata(metadata_location=table)
        else:
            from pyiceberg.catalog import load_catalog

            identifiers = table.split(".")
            if len(identifiers) == 2:
                catalog_name = None
                table_name = table
            elif len(identifiers) >= 3:
                catalog_name = identifiers[0]
                table_name = ".".join(identifiers[1:])
            else:
                raise Exception(
                    f"Invalid table identifier: {table}, must be in form of db.table(use default catalog) or catalog.db.table"
                )

            catalog_config = iceberg_catalog_config or {}
            iceberg_catalog = load_catalog(catalog_name, **catalog_config)
            table = iceberg_catalog.load_table(table_name)

    io_config = (
        _convert_iceberg_file_io_properties_to_io_config(table.io.properties) if io_config is None else io_config
    )
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    multithreaded_io = context.get_context().get_or_create_runner().name != "ray"
    storage_config = StorageConfig(multithreaded_io, io_config)

    iceberg_operator = IcebergScanOperator(table, snapshot_id=snapshot_id, storage_config=storage_config)

    handle = ScanOperatorHandle.from_python_scan_operator(iceberg_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)
