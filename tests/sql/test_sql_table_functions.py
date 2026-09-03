from __future__ import annotations

import contextlib
import io
import os

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft
from daft import DataType as dt
from daft.io import IOConfig, S3Config

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


def test_sql_read_json_array():
    import json

    actual = daft.sql("SELECT * FROM read_json('tests/assets/json-data/sample1.json')")
    df = daft.read_json("tests/assets/json-data/sample1.json")
    with open("tests/assets/json-data/sample1.json") as f:
        expected = json.load(f)

    assert actual.to_pylist() == df.to_pylist()
    assert df.to_pylist() == expected
    assert actual.to_pylist() == df.to_pylist()


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


def test_sql_read_json_array_paths():
    paths = [
        "tests/assets/json-data/sample1.json",
        "tests/assets/json-data/sample2.json",
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


def test_sql_read_parquet_named_path():
    actual = daft.sql("SELECT * FROM read_parquet(path => 'tests/assets/parquet-data/mvp.parquet')")
    expect = daft.read_parquet("tests/assets/parquet-data/mvp.parquet")
    assert actual.to_pydict() == expect.to_pydict()


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


def test_sql_read_parquet_file_options(tmp_path):
    table_dir = tmp_path / "country=us"
    table_dir.mkdir()
    file_path = table_dir / "data.parquet"
    papq.write_table(pa.table({"x": [1, 2]}), file_path)

    glob_path = f"{tmp_path.as_posix()}/**"
    actual = daft.sql(
        f"SELECT * FROM read_parquet('{glob_path}', file_path_column => 'filepath', hive_partitioning => true)"
    )
    expect = daft.read_parquet(glob_path, file_path_column="filepath", hive_partitioning=True)
    assert actual.to_pydict() == expect.to_pydict()


def test_sql_read_parquet_ignore_corrupt_files(tmp_path):
    good_path = tmp_path / "good.parquet"
    bad_path = tmp_path / "bad.parquet"
    papq.write_table(pa.table({"x": [1, 2]}), good_path)
    bad_path.write_bytes(b"PAR1" + b"\x00" * 20 + b"PAR1")

    with pytest.raises(Exception):
        daft.sql(f"SELECT * FROM read_parquet('{tmp_path.as_posix()}')").collect()

    df = daft.sql(f"SELECT * FROM read_parquet('{tmp_path.as_posix()}', ignore_corrupt_files => true)").collect()

    assert df.to_pydict() == {"x": [1, 2]}
    skipped = df.skipped_corrupt_files
    assert len(skipped) == 1
    path, reason, partial = skipped[0]
    assert os.path.basename(path) == "bad.parquet"
    assert reason
    assert not partial


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


def _write_corrupt_csv(path):
    # Binary garbage that is not valid UTF-8, so the CSV reader treats it as corrupt.
    path.write_bytes(b"\x00\x01\x02\x03\xff\xfe\xfd")


def test_sql_read_csv_ignore_corrupt_files(tmp_path):
    good1 = tmp_path / "good1.csv"
    good2 = tmp_path / "good2.csv"
    good1.write_text("a\n1\n2\n3\n")
    good2.write_text("a\n4\n5\n6\n")
    _write_corrupt_csv(tmp_path / "zzz_bad.csv")

    dir_path = tmp_path.as_posix()

    # Without ignore_corrupt_files the corrupt file surfaces as an error.
    with pytest.raises(Exception):
        daft.sql(f"SELECT * FROM read_csv('{dir_path}')").collect()

    # With ignore_corrupt_files => true the corrupt file is skipped and the good
    # rows are returned.
    df = daft.sql(f"SELECT * FROM read_csv('{dir_path}', ignore_corrupt_files => true)").collect()
    assert sorted(df.to_pydict()["a"]) == [1, 2, 3, 4, 5, 6]

    skipped = df.skipped_corrupt_files
    assert len(skipped) == 1
    path, reason, _partial = skipped[0]
    assert path.endswith("zzz_bad.csv")
    assert reason


def _plan_io_config_line(df) -> str | None:
    """Return the plan's ``IO config`` line, or None when the plan carries none."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(show_all=False)
    for line in buf.getvalue().splitlines():
        if "IO config" in line:
            return line.strip()
    return None


@pytest.fixture
def default_io_config():
    """Set a recognizable context default_io_config for the duration of a test."""
    previous = daft.context.get_context().daft_planning_config.default_io_config
    daft.context.set_planning_config(default_io_config=IOConfig(s3=S3Config(region_name="us-west-2")))
    try:
        yield
    finally:
        daft.context.set_planning_config(default_io_config=previous)


@pytest.fixture
def io_config_sample_files(tmp_path):
    parquet_path = tmp_path / "a.parquet"
    papq.write_table(pa.table({"x": [1, 2, 3]}), parquet_path)
    csv_path = tmp_path / "a.csv"
    csv_path.write_text("x\n1\n2\n")
    json_path = tmp_path / "a.jsonl"
    json_path.write_text('{"x": 1}\n{"x": 2}\n')
    return {
        "read_parquet": parquet_path.as_posix(),
        "read_csv": csv_path.as_posix(),
        "read_json": json_path.as_posix(),
    }


@pytest.mark.parametrize("function", ["read_parquet", "read_csv", "read_json"])
def test_sql_reader_uses_default_io_config(default_io_config, io_config_sample_files, function):
    """Without an explicit io_config, SQL readers fall back to the context default.

    The Python readers already do this, so a globally-set ``default_io_config`` used to apply
    to ``daft.read_parquet`` but be silently dropped by ``read_parquet`` in SQL.
    """
    path = io_config_sample_files[function]
    sql_line = _plan_io_config_line(daft.sql(f"SELECT * FROM {function}('{path}')"))
    python_line = _plan_io_config_line(getattr(daft, function)(path))

    assert sql_line is not None
    assert "us-west-2" in sql_line
    assert sql_line == python_line


@pytest.mark.parametrize("function", ["read_parquet", "read_csv", "read_json"])
def test_sql_reader_explicit_io_config_wins(default_io_config, io_config_sample_files, function):
    """An explicit io_config argument overrides the context default."""
    path = io_config_sample_files[function]
    query = f"SELECT * FROM {function}('{path}', io_config := S3Config(region_name => 'eu-central-1'))"

    sql_line = _plan_io_config_line(daft.sql(query))

    assert sql_line is not None
    assert "eu-central-1" in sql_line


@pytest.mark.parametrize(
    "spelling", ["S3Config", "s3config", "S3CONFIG"], ids=["mixed_case", "lower_case", "upper_case"]
)
def test_sql_config_constructors_are_case_insensitive(io_config_sample_files, spelling):
    """Config constructors resolve under any casing, like every other SQL function."""
    path = io_config_sample_files["read_parquet"]
    query = f"SELECT * FROM read_parquet('{path}', io_config := {spelling}(region_name => 'eu-central-1'))"

    sql_line = _plan_io_config_line(daft.sql(query))

    assert sql_line is not None
    assert "eu-central-1" in sql_line
