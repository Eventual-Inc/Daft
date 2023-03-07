from __future__ import annotations

import pathlib

from daft.expressions import ExpressionsProjection
from daft.runners.partitioning import (
    vPartitionParseCSVOptions,
    vPartitionReadOptions,
    vPartitionSchemaInferenceOptions,
)
from daft.table import Table


def read_json(
    path: str,
    read_options: vPartitionReadOptions = vPartitionReadOptions(),
) -> Table:
    raise NotImplementedError("[RUST-INT] Implement tableio")


def read_parquet(
    path: str,
    read_options: vPartitionReadOptions = vPartitionReadOptions(),
) -> Table:
    raise NotImplementedError("[RUST-INT][TPCH] Implement tableio")


def read_csv(
    path: str,
    csv_options: vPartitionParseCSVOptions = vPartitionParseCSVOptions(),
    schema_options: vPartitionSchemaInferenceOptions = vPartitionSchemaInferenceOptions(),
    read_options: vPartitionReadOptions = vPartitionReadOptions(),
) -> Table:
    raise NotImplementedError("[RUST-INT] Implement tableio")


def write_csv(
    table: Table,
    root_path: str | pathlib.Path,
    partition_cols: ExpressionsProjection | None = None,
    compression: str | None = None,
) -> list[str]:
    raise NotImplementedError("[RUST-INT] Implement tableio")


def write_parquet(
    table: Table,
    root_path: str | pathlib.Path,
    partition_cols: ExpressionsProjection | None = None,
    compression: str | None = None,
) -> list[str]:
    raise NotImplementedError("[RUST-INT] Implement tableio")
