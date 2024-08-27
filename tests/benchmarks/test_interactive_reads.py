import pytest

import daft
from daft.io import IOConfig, S3Config


@pytest.fixture
def files(request):
    return request.param


@pytest.fixture
def expected_count(request):
    return request.param


@pytest.fixture
def io_config():
    return IOConfig(s3=S3Config(anonymous=True))


@pytest.mark.benchmark(group="show")
@pytest.mark.parametrize(
    "files",
    [
        pytest.param("s3://daft-public-data/test_fixtures/parquet/small-fake-data.parquet", id="1 Small File"),
        pytest.param(
            100 * ["s3://daft-public-data/test_fixtures/parquet/small-fake-data.parquet"], id="100 Small Files"
        ),
        # pytest.param("s3://daft-public-data/test_fixtures/parquet/large-fake-data.parquet", id="1 Large File"),
        # pytest.param(100 * ["s3://daft-public-data/test_fixtures/parquet/large-fake-data.parquet"], id="100 Large File"),
    ],
    indirect=True,  # This tells pytest to pass the params to the fixture
)
def test_show(files, io_config, benchmark):
    def f():
        df = daft.read_parquet(files, io_config=io_config)
        df.show()

    benchmark(f)


@pytest.mark.benchmark(group="explain")
@pytest.mark.parametrize(
    "files",
    [
        pytest.param("s3://daft-public-data/test_fixtures/parquet/small-fake-data.parquet", id="1 Small File"),
        pytest.param(
            100 * ["s3://daft-public-data/test_fixtures/parquet/small-fake-data.parquet"], id="100 Small Files"
        ),
        # pytest.param("s3://daft-public-data/test_fixtures/parquet/large-fake-data.parquet", id="1 Large File"),
    ],
    indirect=True,  # This tells pytest to pass the params to the fixture
)
def test_explain(files, io_config, benchmark):
    def f():
        df = daft.read_parquet(files, io_config=io_config)
        df.explain(True)

    benchmark(f)


@pytest.mark.benchmark(group="count")
@pytest.mark.parametrize(
    "files, expected_count",
    [
        pytest.param("s3://daft-public-data/test_fixtures/parquet/small-fake-data.parquet", 1024, id="1 Small File"),
        # pytest.param(100*["s3://daft-public-data/test_fixtures/parquet/small-fake-data.parquet"], 100 * 1024, id="100 Small Files"), # Turn this back on after we speed up count
        # pytest.param("s3://daft-public-data/test_fixtures/parquet/large-fake-data.parquet", 100000000, id="1 Large File"),  # Turn this back on after we speed up count
    ],
    indirect=True,  # This tells pytest to pass the params to the fixture
)
def test_count(files, expected_count, io_config, benchmark):
    def f():
        df = daft.read_parquet(files, io_config=io_config)
        return df.count_rows()

    count = benchmark(f)
    assert count == expected_count


@pytest.mark.benchmark(group="iter_rows")
@pytest.mark.parametrize(
    "files",
    [
        pytest.param("s3://daft-public-data/test_fixtures/parquet/small-fake-data.parquet", id="1 Small File"),
        pytest.param(
            100 * ["s3://daft-public-data/test_fixtures/parquet/small-fake-data.parquet"], id="100 Small Files"
        ),
        # pytest.param("s3://daft-public-data/test_fixtures/parquet/large-fake-data.parquet", id="1 Large File"),
    ],
    indirect=True,  # This tells pytest to pass the params to the fixture
)
def test_iter_rows_first_row(files, io_config, benchmark):
    def f():
        df = daft.read_parquet(files, io_config=io_config)
        iter = df.iter_rows()
        return next(iter)

    first_row = benchmark(f)
    assert len(first_row.keys()) == 16
