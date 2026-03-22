# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations


from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import (
    ArrowIpcSourceConfig,
    FileFormatConfig,
    IOConfig,
    StorageConfig,
)
from daft.dataframe import DataFrame
from daft.datatype import DataType
from daft.io.common import get_tabular_files_scan


@PublicAPI
def read_arrow_ipc(
    path: str | list[str],
    infer_schema: bool = True,
    schema: dict[str, DataType] | None = None,
    io_config: IOConfig | None = None,
    file_path_column: str | None = None,
    hive_partitioning: bool = False,
) -> DataFrame:
    """Creates a DataFrame from Apache Arrow IPC file(s) (on-disk IPC file format with footer).

    Args:
        path (str): Path to IPC file(s) (allows for wildcards; supports remote URLs such as ``s3://`` or ``gs://``).
        infer_schema (bool): Whether to infer the schema from the first matching file. Defaults to True.
        schema (dict[str, DataType]): If ``infer_schema`` is False, the definitive schema; if True, optional hints applied after inference.
        io_config (IOConfig): Config for the native IO client.
        file_path_column: If set, adds a column with this name containing each file's path.
        hive_partitioning: Whether to parse Hive-style path partitions into columns.

    Returns:
        DataFrame: Data read from Arrow IPC file(s).

    Examples:
        >>> import pyarrow as pa
        >>> table = pa.table({"x": [1, 2]})
        >>> with pa.OSFile("data.arrow", "wb") as sink:
        ...     with pa.ipc.new_file(sink, table.schema) as writer:
        ...         writer.write_table(table)
        >>> df = daft.read_arrow_ipc("data.arrow")  # doctest: +SKIP
    """
    if isinstance(path, list) and len(path) == 0:
        raise ValueError("Cannot read DataFrame from empty list of Arrow IPC file paths")

    if not infer_schema and schema is None:
        raise ValueError(
            "Cannot read DataFrame with infer_schema=False and schema=None, please provide a schema or set infer_schema=True"
        )

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    ipc_config = ArrowIpcSourceConfig()
    file_format_config = FileFormatConfig.from_arrow_ipc_config(ipc_config)
    storage_config = StorageConfig(True, io_config)

    builder = get_tabular_files_scan(
        path=path,
        infer_schema=infer_schema,
        schema=schema,
        file_format_config=file_format_config,
        storage_config=storage_config,
        file_path_column=file_path_column,
        hive_partitioning=hive_partitioning,
    )
    return DataFrame(builder)
