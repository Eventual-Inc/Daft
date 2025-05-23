from __future__ import annotations

import pytest

import daft
from daft import DataType as dt

# TODO chore: make an asset fixture for all tests (beyond just sql).


@pytest.fixture
def sample_csv_path():
    return "tests/assets/mvp.csv"


@pytest.fixture
def sample_schema():
    return {"a": daft.DataType.float32(), "b": daft.DataType.string()}


def assert_eq(actual, expect):
    actual.to_pydict() == expect.to_pydict()


def to_sql_array(paths: list[str]) -> str:
    return "[ " + ", ".join([f"'{p}'" for p in paths]) + " ]"


def test_sql_read_json():
    actual = daft.sql("SELECT * FROM read_json('tests/assets/json-data/sample1.jsonl')")
    expect = daft.read_json("tests/assets/json-data/sample1.jsonl")
    assert_eq(actual, expect)


def test_sql_read_json_path():
    actual = daft.sql("SELECT * FROM 'tests/assets/json-data/sample1.jsonl'")
    expect = daft.read_json("tests/assets/json-data/sample1.jsonl")
    assert_eq(actual, expect)


def test_sql_read_json_paths():
    paths = [
        "tests/assets/json-data/sample1.jsonl",
        "tests/assets/json-data/sample2.jsonl",
    ]
    actual = daft.sql(f"SELECT * FROM read_json({to_sql_array(paths)})")
    expect = daft.read_json(paths)
    assert_eq(actual, expect)


def test_sql_read_with_schema():
    actual = daft.sql("""SELECT * FROM read_json('tests/assets/json-data/sample1.jsonl', schema := {
        'x': 'int',
        'y': 'string',
        'z': 'bool',
    });""")
    expect = daft.read_json(
        "tests/assets/json-data/sample1.jsonl",
        schema={
            "x": dt.int32(),
            "y": dt.string(),
            "z": dt.bool(),
        },
    )
    assert_eq(actual, expect)


def test_sql_read_parquet():
    actual = daft.sql("SELECT * FROM read_parquet('tests/assets/parquet-data/mvp.parquet')")
    expect = daft.read_parquet("tests/assets/parquet-data/mvp.parquet")
    assert_eq(actual, expect)


def test_sql_read_parquet_path():
    actual = daft.sql("SELECT * FROM 'tests/assets/parquet-data/mvp.parquet'")
    expect = daft.read_parquet("tests/assets/parquet-data/mvp.parquet")
    assert_eq(actual, expect)


def test_sql_read_parquet_paths():
    paths = [
        "tests/assets/parquet-data/mvp.parquet",
        "tests/assets/parquet-data/parquet-with-schema-metadata.parquet",
    ]
    actual = daft.sql(f"SELECT * FROM read_parquet({to_sql_array(paths)})")
    expect = daft.read_parquet(paths)
    assert_eq(actual, expect)


def test_sql_read_csv(sample_csv_path):
    actual = daft.sql(f"SELECT * FROM read_csv('{sample_csv_path}')")
    expect = daft.read_csv(sample_csv_path)
    assert_eq(actual, expect)


def test_sql_read_csv_path(sample_csv_path):
    actual = daft.sql(f"SELECT * FROM '{sample_csv_path}'")
    expect = daft.read_csv(sample_csv_path)
    assert_eq(actual, expect)


def test_sql_read_csv_paths():
    paths = ["tests/assets/mvp.csv", "tests/assets/sampled-tpch.csv"]
    actual = daft.sql(f"SELECT * FROM read_csv({to_sql_array(paths)})")
    expect = daft.read_csv(paths)
    assert_eq(actual, expect)


def test_sql_read_csv_with_schema(sample_csv_path):
    actual = daft.sql("""SELECT * FROM read_csv('tests/assets/mvp.csv', schema := {
        'a': 'double',
        'b': 'string',
    });""")
    expect = daft.read_csv(
        sample_csv_path,
        schema={
            "a": dt.float64(),
            "b": dt.string(),
        },
    )
    assert_eq(actual, expect)


@pytest.mark.parametrize("has_headers", [True, False])
def test_read_csv_headers(sample_csv_path, has_headers):
    df1 = daft.read_csv(sample_csv_path, has_headers=has_headers)
    df2 = daft.sql(f"SELECT * FROM read_csv('{sample_csv_path}', has_headers => {str(has_headers).lower()})")
    assert_eq(df2, df1)


@pytest.mark.parametrize("double_quote", [True, False])
def test_read_csv_quote(sample_csv_path, double_quote):
    df1 = daft.read_csv(sample_csv_path, double_quote=double_quote)
    df2 = daft.sql(f"SELECT * FROM read_csv('{sample_csv_path}', double_quote => {str(double_quote).lower()})")
    assert_eq(df2, df1)


@pytest.mark.parametrize("op", ["=>", ":="])
def test_read_csv_other_options(
    sample_csv_path,
    op,
    delimiter=",",
    escape_char="\\",
    comment="#",
    allow_variable_columns=True,
    file_path_column="filepath",
    hive_partitioning=False,
):
    df1 = daft.read_csv(
        sample_csv_path,
        delimiter=delimiter,
        escape_char=escape_char,
        comment=comment,
        allow_variable_columns=allow_variable_columns,
        file_path_column=file_path_column,
        hive_partitioning=hive_partitioning,
    )
    df2 = daft.sql(
        f"SELECT * FROM read_csv('{sample_csv_path}', delimiter {op} '{delimiter}', escape_char {op} '{escape_char}', comment {op} '{comment}', allow_variable_columns {op} {str(allow_variable_columns).lower()}, file_path_column {op} '{file_path_column}', hive_partitioning {op} {str(hive_partitioning).lower()})"
    )
    assert_eq(df2, df1)


def test_sql_read_path_no_alias(sample_csv_path):
    # don't allow using paths as table names
    with pytest.raises(Exception, match="Table not found"):
        daft.sql(f""" SELECT "{sample_csv_path}".* FROM '{sample_csv_path}' """)
