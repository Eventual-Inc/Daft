# isort: dont-add-import: from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional

from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, ScanOperatorHandle
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder

if TYPE_CHECKING:
    from pyiceberg.table import Table as PyIcebergTable

from pyiceberg.io import (
    GCS_PROJECT_ID,
    S3_ACCESS_KEY_ID,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_ACCESS_KEY,
    S3_SESSION_TOKEN,
)


def _convert_iceberg_file_io_properties_to_io_config(props: Dict[str, Any]) -> Optional["IOConfig"]:
    from daft.io import GCSConfig, IOConfig, S3Config

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

    gcs_mapping = {
        GCS_PROJECT_ID: "project_id",
    }
    gcs_args = dict()  # type: ignore
    for pyiceberg_key, daft_key in gcs_mapping.items():
        value = props.get(pyiceberg_key, None)
        if value is not None:
            gcs_args[daft_key] = value

    if len(gcs_args) > 0:
        gcs_config = GCSConfig(**gcs_args)
    else:
        gcs_config = None

    if s3_config is not None or gcs_config is not None:
        return IOConfig(s3=s3_config, gcs=gcs_config)
    else:
        return None


@PublicAPI
def read_iceberg(
    table: "PyIcebergTable",
    io_config: Optional["IOConfig"] = None,
) -> DataFrame:
    from daft.iceberg.iceberg_scan import IcebergScanOperator

    if io_config is None:
        io_config = _convert_iceberg_file_io_properties_to_io_config(table.io.properties)
    iceberg_operator = IcebergScanOperator(table, io_config=io_config)
    handle = ScanOperatorHandle.from_python_scan_operator(iceberg_operator)
    builder = LogicalPlanBuilder.from_tabular_scan_with_scan_operator(
        scan_operator=handle, schema_hint=iceberg_operator.schema()
    )
    return DataFrame(builder)
