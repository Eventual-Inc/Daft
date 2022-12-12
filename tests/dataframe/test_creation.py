from __future__ import annotations

import csv
import json
import tempfile

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

from daft.dataframe import DataFrame

COL_NAMES = [
    "sepal_length",
    "sepal_width",
    "petal_length",
    "petal_width",
    "variety",
]


def test_create_dataframe(valid_data: list[dict[str, float]]) -> None:
    df = DataFrame.from_pylist(valid_data)
    assert df.column_names == COL_NAMES


def test_create_dataframe_pydict(valid_data: list[dict[str, float]]) -> None:
    pydict = {k: [item[k] for item in valid_data] for k in valid_data[0].keys()}
    df = DataFrame.from_pydict(pydict)
    assert df.column_names == COL_NAMES


###
# CSV tests
###


def test_create_dataframe_csv(valid_data: list[dict[str, float]]) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        header = list(valid_data[0].keys())
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows([[item[col] for col in header] for item in valid_data])
        f.flush()

        df = DataFrame.read_csv(f.name)
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)


@pytest.mark.parametrize("has_headers", [True, False])
def test_create_dataframe_csv_provide_headers(valid_data: list[dict[str, float]], has_headers: bool) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        header = list(valid_data[0].keys())
        writer = csv.writer(f)
        if has_headers:
            writer.writerow(header)
        writer.writerows([[item[col] for col in header] for item in valid_data])
        f.flush()

        cnames = [f"foo{i}" for i in range(5)]
        df = DataFrame.read_csv(f.name, has_headers=has_headers, column_names=cnames)
        assert df.column_names == cnames

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == cnames
        assert len(pd_df) == len(valid_data)


def test_create_dataframe_csv_generate_headers(valid_data: list[dict[str, float]]) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        header = list(valid_data[0].keys())
        writer = csv.writer(f)
        writer.writerows([[item[col] for col in header] for item in valid_data])
        f.flush()

        cnames = [f"f{i}" for i in range(5)]
        df = DataFrame.read_csv(f.name, has_headers=False)
        assert df.column_names == cnames

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == cnames
        assert len(pd_df) == len(valid_data)


def test_create_dataframe_csv_column_projection(valid_data: list[dict[str, float]]) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        header = list(valid_data[0].keys())
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows([[item[col] for col in header] for item in valid_data])
        f.flush()

        col_subset = COL_NAMES[:3]

        df = DataFrame.read_csv(f.name)
        df = df.select(*col_subset)
        assert df.column_names == col_subset

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == col_subset
        assert len(pd_df) == len(valid_data)


def test_create_dataframe_csv_custom_delimiter(valid_data: list[dict[str, float]]) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        header = list(valid_data[0].keys())
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(header)
        writer.writerows([[item[col] for col in header] for item in valid_data])
        f.flush()

        df = DataFrame.read_csv(f.name, delimiter="\t")
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)


###
# JSON tests
###


def test_create_dataframe_json(valid_data: list[dict[str, float]]) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        for data in valid_data:
            f.write(json.dumps(data))
            f.write("\n")
        f.flush()

        df = DataFrame.read_json(f.name)
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)


def test_create_dataframe_json_column_projection(valid_data: list[dict[str, float]]) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        for data in valid_data:
            f.write(json.dumps(data))
            f.write("\n")
        f.flush()

        col_subset = COL_NAMES[:3]

        df = DataFrame.read_json(f.name)
        df = df.select(*col_subset)
        assert df.column_names == col_subset

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == col_subset
        assert len(pd_df) == len(valid_data)


###
# Parquet tests
###


def test_create_dataframe_parquet(valid_data: list[dict[str, float]]) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        table = pa.Table.from_pydict({col: [d[col] for d in valid_data] for col in COL_NAMES})
        papq.write_table(table, f.name)
        f.flush()

        df = DataFrame.read_parquet(f.name)
        assert df.column_names == COL_NAMES

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == COL_NAMES
        assert len(pd_df) == len(valid_data)


def test_create_dataframe_parquet_column_projection(valid_data: list[dict[str, float]]) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        table = pa.Table.from_pydict({col: [d[col] for d in valid_data] for col in COL_NAMES})
        papq.write_table(table, f.name)
        f.flush()

        col_subset = COL_NAMES[:3]

        df = DataFrame.read_parquet(f.name)
        df = df.select(*col_subset)
        assert df.column_names == col_subset

        pd_df = df.to_pandas()
        assert list(pd_df.columns) == col_subset
        assert len(pd_df) == len(valid_data)
