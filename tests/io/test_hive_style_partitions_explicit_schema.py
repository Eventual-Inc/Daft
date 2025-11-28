from __future__ import annotations

import os
from datetime import date

import pyarrow as pa
import pytest

import daft

NUM_ROWS = 12
SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("value", pa.int64()),
        ("str_col", pa.large_string()),
        ("int_col", pa.int64()),
        ("date_col", pa.date32()),
    ]
)
SAMPLE_DATA = pa.table(
    {
        "id": list(range(NUM_ROWS)),
        "value": list(range(NUM_ROWS)),
        "str_col": [f"str{i % 2}" for i in range(NUM_ROWS)],
        "int_col": [i % 2 for i in range(NUM_ROWS)],
        "date_col": [date(2024, 1, (i % 2) + 1) for i in range(NUM_ROWS)],
    },
    schema=SCHEMA,
)


@pytest.mark.parametrize("file_format", ["csv", "parquet", "json"])
@pytest.mark.parametrize("partition_by", [["str_col"], ["int_col"], ["date_col"], ["str_col", "int_col"]])
def test_explicit_schema_preserves_hive_partitions(tmpdir, file_format, partition_by):
    base = daft.from_arrow(SAMPLE_DATA)
    # Write partitioned dataset
    if file_format == "parquet":
        base.write_parquet(str(tmpdir), partition_cols=partition_by)
    elif file_format == "csv":
        base.write_csv(str(tmpdir), partition_cols=partition_by)
    elif file_format == "json":
        base.write_json(str(tmpdir), partition_cols=partition_by)
    else:
        pytest.skip("Unsupported format for this test")

    glob_path = os.path.join(str(tmpdir), "**")

    # Provide explicit schema ONLY for file-content columns when infer_schema=False
    schema_hint = {
        "id": daft.DataType.int64(),
        "value": daft.DataType.int64(),
    }

    df = ()
    if file_format == "csv":
        df = daft.read_csv(glob_path, infer_schema=False, schema=schema_hint, hive_partitioning=True)
    elif file_format == "parquet":
        df = daft.read_parquet(glob_path, infer_schema=False, schema=schema_hint, hive_partitioning=True)
    elif file_format == "json":
        df = daft.read_json(glob_path, infer_schema=False, schema=schema_hint, hive_partitioning=True)
    else:
        pytest.skip("Unsupported format for this test")

    # Ensure partition columns are present and have expected values
    pdf = df.to_pydict()
    for col in partition_by:
        assert col in df.column_names, f"Missing partition column {col} when infer_schema=False"
        assert set(pdf[col]) == set(SAMPLE_DATA[col].to_pylist())


@pytest.mark.parametrize("file_format", ["csv", "parquet", "json"])
def test_explicit_schema_preserves_file_path_column(tmpdir, file_format):
    # Write non-partitioned dataset
    base = daft.from_arrow(SAMPLE_DATA)
    if file_format == "csv":
        base.write_csv(str(tmpdir))
    elif file_format == "parquet":
        base.write_parquet(str(tmpdir))
    elif file_format == "json":
        base.write_json(str(tmpdir))
    else:
        pytest.skip("Unsupported format for this test")

    glob_path = os.path.join(str(tmpdir), "**")

    # Provide explicit schema ONLY for file-content columns when infer_schema=False
    schema_hint = {
        "id": daft.DataType.int64(),
        "value": daft.DataType.int64(),
    }

    df = ()
    if file_format == "csv":
        df = daft.read_csv(glob_path, infer_schema=False, schema=schema_hint, file_path_column="path")
    elif file_format == "parquet":
        df = daft.read_parquet(glob_path, infer_schema=False, schema=schema_hint, file_path_column="path")
    elif file_format == "json":
        df = daft.read_json(glob_path, infer_schema=False, schema=schema_hint, file_path_column="path")
    else:
        pytest.skip("Unsupported format for this test")

    assert "path" in df.column_names, "Missing file path column when infer_schema=False"
    paths = df.to_pydict()["path"]
    assert all(isinstance(p, str) and len(p) > 0 for p in paths)
