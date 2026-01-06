from __future__ import annotations

import json
import os

import pytest

import daft
from daft import DataType as dt

# TODO chore: make an asset fixture for all tests (beyond just sql).


@pytest.fixture
def sample_schema():
    return {"a": daft.DataType.float32(), "b": daft.DataType.string()}


def assert_eq(actual, expect):
    actual.to_pydict() == expect.to_pydict()


def to_sql_array(paths: list[str]) -> str:
    return "[ " + ", ".join([f"'{p}'" for p in paths]) + " ]"


def test_sql_read_json(assets_path):
    path = os.path.join(assets_path, "json-data", "sample1.jsonl")
    actual = daft.sql(f"SELECT * FROM read_json('{path}')")
    expect = daft.read_json(path)
    assert_eq(actual, expect)


def test_sql_read_json_array(assets_path):
    path = os.path.join(assets_path, "json-data", "sample1.json")

    actual = daft.sql(f"SELECT * FROM read_json('{path}')")
    df = daft.read_json(path)
    with open(path) as f:
        expected = json.load(f)

    assert actual.to_pylist() == df.to_pylist()
    assert df.to_pylist() == expected
    assert actual.to_pylist() == df.to_pylist()


def test_sql_read_json_path(assets_path):
    path = os.path.join(assets_path, "json-data", "sample1.jsonl")
    actual = daft.sql(f"SELECT * FROM '{path}'")
    expect = daft.read_json(path)
    assert_eq(actual, expect)


def test_sql_read_json_paths(assets_path):
    paths = [
        os.path.join(assets_path, "json-data", "sample1.jsonl"),
        os.path.join(assets_path, "json-data", "sample2.jsonl"),
    ]
    actual = daft.sql(f"SELECT * FROM read_json({to_sql_array(paths)})")
    expect = daft.read_json(paths)
    assert_eq(actual, expect)


def test_sql_read_json_array_paths(assets_path):
    paths = [
        os.path.join(assets_path, "json-data", "sample1.json"),
        os.path.join(assets_path, "json-data", "sample2.json"),
    ]
    actual = daft.sql(f"SELECT * FROM read_json({to_sql_array(paths)})")
    expect = daft.read_json(paths)
    assert_eq(actual, expect)


def test_sql_read_with_schema(assets_path):
    path = os.path.join(assets_path, "json-data", "sample1.jsonl")
    actual = daft.sql(f"""SELECT * FROM read_json('{path}', schema := {{
        'x': 'int',
        'y': 'string',
        'z': 'bool',
    }});""")
    expect = daft.read_json(
        path,
        schema={
            "x": dt.int32(),
            "y": dt.string(),
            "z": dt.bool(),
        },
    )
    assert_eq(actual, expect)


def test_sql_read_parquet(mvp_parquet_path):
    actual = daft.sql(f"SELECT * FROM read_parquet('{mvp_parquet_path}')")
    expect = daft.read_parquet(mvp_parquet_path)
    assert_eq(actual, expect)


def test_sql_read_parquet_path(mvp_parquet_path):
    actual = daft.sql(f"SELECT * FROM '{mvp_parquet_path}'")
    expect = daft.read_parquet(mvp_parquet_path)
    assert_eq(actual, expect)


def test_sql_read_parquet_paths(mvp_parquet_path, assets_path):
    paths = [
        mvp_parquet_path,
        os.path.join(assets_path, "parquet-data", "parquet-with-schema-metadata.parquet"),
    ]
    actual = daft.sql(f"SELECT * FROM read_parquet({to_sql_array(paths)})")
    expect = daft.read_parquet(paths)
    assert_eq(actual, expect)


def test_sql_read_csv(mvp_csv_path):
    actual = daft.sql(f"SELECT * FROM read_csv('{mvp_csv_path}')")
    expect = daft.read_csv(mvp_csv_path)
    assert_eq(actual, expect)


def test_sql_read_csv_path(mvp_csv_path):
    actual = daft.sql(f"SELECT * FROM '{mvp_csv_path}'")
    expect = daft.read_csv(mvp_csv_path)
    assert_eq(actual, expect)


def test_sql_read_csv_paths(mvp_csv_path, assets_path):
    paths = [mvp_csv_path, os.path.join(assets_path, "sampled-tpch.csv")]
    actual = daft.sql(f"SELECT * FROM read_csv({to_sql_array(paths)})")
    expect = daft.read_csv(paths)
    assert_eq(actual, expect)


def test_sql_read_csv_with_schema(mvp_csv_path):
    actual = daft.sql(f"""SELECT * FROM read_csv('{mvp_csv_path}', schema := {{
        'a': 'double',
        'b': 'string',
    }});""")
    expect = daft.read_csv(
        mvp_csv_path,
        schema={
            "a": dt.float64(),
            "b": dt.string(),
        },
    )
    assert_eq(actual, expect)


@pytest.mark.parametrize("has_headers", [True, False])
def test_read_csv_headers(mvp_csv_path, has_headers):
    df1 = daft.read_csv(mvp_csv_path, has_headers=has_headers)
    df2 = daft.sql(f"SELECT * FROM read_csv('{mvp_csv_path}', has_headers => {str(has_headers).lower()})")
    assert_eq(df2, df1)


@pytest.mark.parametrize("double_quote", [True, False])
def test_read_csv_quote(mvp_csv_path, double_quote):
    df1 = daft.read_csv(mvp_csv_path, double_quote=double_quote)
    df2 = daft.sql(f"SELECT * FROM read_csv('{mvp_csv_path}', double_quote => {str(double_quote).lower()})")
    assert_eq(df2, df1)


@pytest.mark.parametrize("op", ["=>", ":="])
def test_read_csv_other_options(
    mvp_csv_path,
    op,
    delimiter=",",
    escape_char="\\",
    comment="#",
    allow_variable_columns=True,
    file_path_column="filepath",
    hive_partitioning=False,
):
    df1 = daft.read_csv(
        mvp_csv_path,
        delimiter=delimiter,
        escape_char=escape_char,
        comment=comment,
        allow_variable_columns=allow_variable_columns,
        file_path_column=file_path_column,
        hive_partitioning=hive_partitioning,
    )
    df2 = daft.sql(
        f"SELECT * FROM read_csv('{mvp_csv_path}', delimiter {op} '{delimiter}', escape_char {op} '{escape_char}', comment {op} '{comment}', allow_variable_columns {op} {str(allow_variable_columns).lower()}, file_path_column {op} '{file_path_column}', hive_partitioning {op} {str(hive_partitioning).lower()})"
    )
    assert_eq(df2, df1)


def test_sql_read_path_no_alias(mvp_csv_path):
    # don't allow using paths as table names
    with pytest.raises(Exception, match="Table not found"):
        daft.sql(f""" SELECT "{mvp_csv_path}".* FROM '{mvp_csv_path}' """)
