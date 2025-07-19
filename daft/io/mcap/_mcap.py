from __future__ import annotations

from daft import DataFrame
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, ScanOperatorHandle
from daft.logical.builder import LogicalPlanBuilder


@PublicAPI
def read_mcap(
    path: str,
    io_config: IOConfig | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
    topics: list[str] | None = None,
    batch_size: int = 1000,
) -> DataFrame:
    """Read mcap file.

    Args:
        path: mcap file path
        start_time: Start time in milliseconds to filter messages.
        end_time: End time in milliseconds to filter messages.
        topics: List of topics to filter messages.
        batch_size: Number of messages to read in each batch.

    Returns:
        DataFrame: DataFrame with the schema converted from the specified MCAP file.
    """
    from .mcap_scan import MCAPScanOperator

    scan_operator = MCAPScanOperator(
        file_path=path,
        start_time=start_time,
        end_time=end_time,
        topics=topics,
        batch_size=batch_size,
        io_config=io_config,
    )

    handle = ScanOperatorHandle.from_python_scan_operator(scan_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)
