"""TODO: https://github.com/Eventual-Inc/Daft/issues/378
The functionality in this module should be implemented in Rust, so that PyArrow is not a Python dependency.
"""

from __future__ import annotations

import io
from functools import partial
from typing import IO, Callable

import pyarrow as pa
from pyarrow import csv, json
from pyarrow import parquet as papq

from daft.expressions import ColumnExpression
from daft.filesystem import get_filesystem_from_path
from daft.logical.schema import ExpressionList
from daft.types import ExpressionType


def _sample_with_pyarrow(
    loader_func: Callable[[IO], pa.Table],
    filepath: str,
    max_bytes: int = 5 * 1024**2,
) -> ExpressionList:
    """Helper to sample a Daft logical schema using a PyArrow and a function to load a PyArrow table from a file

    Args:
        loader_func (Callable[[IO], pa.Table]): function to load PyArrow table from a file
        filepath (str): path to file
        max_bytes (int, optional): maximum number of bytes to read from file. Defaults to 5MB.

    Returns:
        ExpressionList: Sampled schema
    """
    fs = get_filesystem_from_path(filepath)
    sampled_bytes = io.BytesIO()
    with fs.open(filepath, compression="infer") as f:
        lines = f.readlines(max_bytes)
        for line in lines:
            sampled_bytes.write(line)
    sampled_bytes.seek(0)
    sampled_tbl = loader_func(sampled_bytes)
    fields = [(field.name, field.type) for field in sampled_tbl.schema]
    schema = ExpressionList(
        [ColumnExpression(name, expr_type=ExpressionType.from_arrow_type(type_)) for name, type_ in fields]
    )
    return schema


def sample_json(filepaths: list[str], max_bytes: int = 5 * 1024**2) -> ExpressionList:
    """Samples a Daft logical schema from a JSON file."""
    if len(filepaths) == 0:
        raise ValueError("No files provided to sample")

    # TODO: We currently only read the first file to sample the schema, but in the future could do more sophisticated
    # reads in parallel and a schema resolution depending on current runner
    filepath = filepaths[0]

    return _sample_with_pyarrow(json.read_json, filepath, max_bytes)


def sample_csv(
    filepaths: list[str],
    delimiter: str,
    has_headers: bool,
    column_names: list[str] | None,
    max_bytes: int = 5 * 1024**2,
) -> ExpressionList:
    """Samples a Daft logical schema from a CSV file."""
    if len(filepaths) == 0:
        raise ValueError("No files provided to sample")

    # TODO: We currently only read the first file to sample the schema, but in the future could do more sophisticated
    # reads in parallel and a schema resolution depending on current runner
    filepath = filepaths[0]

    return _sample_with_pyarrow(
        partial(
            csv.read_csv,
            parse_options=csv.ParseOptions(
                delimiter=delimiter,
            ),
            read_options=csv.ReadOptions(
                # Column names will be read from the first CSV row if column_names is None/empty and has_headers
                autogenerate_column_names=(not has_headers) and (column_names is None),
                column_names=column_names,
                # If user specifies that CSV has headers, and also provides column names, we skip the header row
                skip_rows_after_names=1 if has_headers and column_names is not None else 0,
            ),
        ),
        filepath,
    )


def sample_parquet(filepaths: list[str]) -> ExpressionList:
    """Samples a Daft logical schema from a Parquet file."""
    if len(filepaths) == 0:
        raise ValueError("No files provided to sample")

    # TODO: We currently only read the first file to sample the schema, but in the future could do more sophisticated
    # reads in parallel and a schema resolution depending on current runner
    filepath = filepaths[0]

    fs = get_filesystem_from_path(filepath)
    with fs.open(filepath, "rb") as f:
        return ExpressionList(
            [
                ColumnExpression(field.name, expr_type=ExpressionType.from_arrow_type(field.type))
                for field in papq.ParquetFile(f).metadata.schema.to_arrow_schema()
            ]
        )
