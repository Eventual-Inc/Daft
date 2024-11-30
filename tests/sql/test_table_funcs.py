import pytest

import daft


@pytest.fixture
def sample_csv_path():
    return "tests/assets/mvp.csv"


@pytest.fixture
def sample_schema():
    return {"a": daft.DataType.float32(), "b": daft.DataType.string()}


def test_sql_read_parquet():
    df = daft.sql("SELECT * FROM read_parquet('tests/assets/parquet-data/mvp.parquet')").collect()
    expected = daft.read_parquet("tests/assets/parquet-data/mvp.parquet").collect()
    assert df.to_pydict() == expected.to_pydict()


def test_sql_read_csv(sample_csv_path):
    df = daft.sql(f"SELECT * FROM read_csv('{sample_csv_path}')").collect()
    expected = daft.read_csv(sample_csv_path).collect()
    assert df.to_pydict() == expected.to_pydict()


@pytest.mark.parametrize("has_headers", [True, False])
def test_read_csv_headers(sample_csv_path, has_headers):
    df1 = daft.read_csv(sample_csv_path, has_headers=has_headers)
    df2 = daft.sql(f"SELECT * FROM read_csv('{sample_csv_path}', has_headers => {str(has_headers).lower()})").collect()
    assert df1.to_pydict() == df2.to_pydict()


@pytest.mark.parametrize("double_quote", [True, False])
def test_read_csv_quote(sample_csv_path, double_quote):
    df1 = daft.read_csv(sample_csv_path, double_quote=double_quote)
    df2 = daft.sql(
        f"SELECT * FROM read_csv('{sample_csv_path}', double_quote => {str(double_quote).lower()})"
    ).collect()
    assert df1.to_pydict() == df2.to_pydict()


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
    use_native_downloader=True,
):
    df1 = daft.read_csv(
        sample_csv_path,
        delimiter=delimiter,
        escape_char=escape_char,
        comment=comment,
        allow_variable_columns=allow_variable_columns,
        file_path_column=file_path_column,
        hive_partitioning=hive_partitioning,
        use_native_downloader=use_native_downloader,
    )
    df2 = daft.sql(
        f"SELECT * FROM read_csv('{sample_csv_path}', delimiter {op} '{delimiter}', escape_char {op} '{escape_char}', comment {op} '{comment}', allow_variable_columns {op} {str(allow_variable_columns).lower()}, file_path_column {op} '{file_path_column}', hive_partitioning {op} {str(hive_partitioning).lower()}, use_native_downloader {op} {str(use_native_downloader).lower()})"
    ).collect()
    assert df1.to_pydict() == df2.to_pydict()
