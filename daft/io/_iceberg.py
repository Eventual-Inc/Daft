# isort: dont-add-import: from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from daft.api_annotations import PublicAPI
from daft.daft import (
    IOConfig,
    ScanOperatorHandle,
)
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder

if TYPE_CHECKING:
    from pyiceberg.table import Table as PyIcebergTable


@PublicAPI
def read_iceberg(
    table: "PyIcebergTable",
    io_config: Optional["IOConfig"] = None,
) -> DataFrame:
    from daft.iceberg.iceberg_scan import IcebergScanOperator

    iceberg_operator = IcebergScanOperator(table, io_config=io_config)
    handle = ScanOperatorHandle.from_python_abc(iceberg_operator)
    builder = LogicalPlanBuilder.from_tabular_scan_with_scan_operator(
        scan_operator=handle, schema_hint=iceberg_operator.schema()
    )
    return DataFrame(builder)
