import daft
import pytest


@pytest.fixture
def sample_csv_path():
    return "tests/assets/mvp.csv"

@pytest.fixture
def sample_schema():
    return {"a": "Float32", "b": "Utf8"}


def test_sql_read_parquet():
    df = daft.sql("SELECT * FROM read_parquet('tests/assets/parquet-data/mvp.parquet')").collect()
    expected = daft.read_parquet("tests/assets/parquet-data/mvp.parquet").collect()
    assert df.to_pydict() == expected.to_pydict()


def test_sql_read_csv():
    df = daft.sql("SELECT * FROM read_csv('tests/assets/mvp.csv')").collect()
    expected = daft.read_csv("tests/assets/mvp.csv").collect()
    assert df.to_pydict() == expected.to_pydict()


@pytest.mark.parametrize("has_headers", [True, False])
def test_read_csv_headers(sample_csv_path, has_headers):
    df1 = daft.read_csv(sample_csv_path, has_headers=has_headers)
    df2 = daft.sql(f"SELECT * FROM read_csv('{sample_csv_path}', has_headers => {str(has_headers).lower()})").collect()
    assert df1.to_pydict() == df2.to_pydict()

@pytest.mark.parametrize("delimiter", [","])
def test_read_csv_delimiter(sample_csv_path, delimiter):
    df1 = daft.read_csv(sample_csv_path, delimiter=delimiter)
    df2 = daft.sql(f"SELECT * FROM read_csv('{sample_csv_path}', delimiter => '{delimiter}')").collect()
    assert df1.to_pydict() == df2.to_pydict()

@pytest.mark.parametrize("double_quote", [True, False])
def test_read_csv_quote(sample_csv_path, double_quote):
    df1 = daft.read_csv(sample_csv_path, double_quote=double_quote)
    df2 = daft.sql(f"SELECT * FROM read_csv('{sample_csv_path}', double_quote => {str(double_quote).lower()})").collect()
    assert df1.to_pydict() == df2.to_pydict()

def test_read_csv_schema(sample_csv_path, sample_schema):
    df1 = daft.read_csv(sample_csv_path, schema={"a": daft.DataType.float32(), "b": daft.DataType.string()})
    df2 = daft.sql(f"SELECT * FROM read_csv('{sample_csv_path}', schema => {sample_schema})").collect()
    assert df1.to_pydict() == df2.to_pydict()

@pytest.mark.parametrize("escape_char, comment, allow_variable_columns, file_path_column, hive_partitioning, use_native_downloader", [
    ("\\", "#", True, "filepath", False, True)
])
def test_read_csv_other_options(sample_csv_path, escape_char, comment, allow_variable_columns, file_path_column, hive_partitioning, use_native_downloader):
    df1 = daft.read_csv(sample_csv_path, escape_char=escape_char, comment=comment, allow_variable_columns=allow_variable_columns, file_path_column=file_path_column, hive_partitioning=hive_partitioning, use_native_downloader=use_native_downloader)
    df2 = daft.sql(f"SELECT * FROM read_csv('{sample_csv_path}', escape_char => '{escape_char}', comment => '{comment}', allow_variable_columns => {str(allow_variable_columns).lower()}, file_path_column => '{file_path_column}', hive_partitioning => {str(hive_partitioning).lower()}, use_native_downloader => {str(use_native_downloader).lower()})").collect()
    assert df1.to_pydict() == df2.to_pydict()