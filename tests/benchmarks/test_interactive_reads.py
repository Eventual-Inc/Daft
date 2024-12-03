import dataclasses
import tempfile

import boto3
import pytest
from botocore import UNSIGNED
from botocore.client import Config

import daft
from daft.io import IOConfig, S3Config


@pytest.fixture
def expected_count(request):
    return request.param


@pytest.fixture
def io_config():
    return IOConfig(s3=S3Config(anonymous=True))


@dataclasses.dataclass(frozen=True)
class FileRequest:
    num_files: int
    small: bool


@pytest.fixture(scope="session")
def files(request):
    file_request = request.param
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    if file_request.small:
        with tempfile.TemporaryDirectory() as tmpdir:
            local_file = f"{tmpdir}/small-fake-data.parquet.parquet"
            s3.download_file("daft-public-data", "test_fixtures/parquet/small-fake-data.parquet", local_file)
            if file_request.num_files == 1:
                yield local_file
            else:
                yield [local_file] * file_request.num_files

    else:
        remote_large_file = "s3://daft-public-data/test_fixtures/parquet/large-fake-data.parquet"
        if file_request.num_files == 1:
            yield remote_large_file
        else:
            yield [remote_large_file] * file_request.num_files


@pytest.mark.benchmark(group="show")
@pytest.mark.parametrize(
    "files",
    [
        pytest.param(FileRequest(num_files=1, small=True), id="1 Small File"),
        pytest.param(FileRequest(num_files=100, small=True), id="100 Small Files"),
        pytest.param(FileRequest(num_files=1, small=False), id="1 Large File"),
        pytest.param(FileRequest(num_files=20, small=False), id="20 Large Files"),
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
        pytest.param(FileRequest(num_files=1, small=True), id="1 Small File"),
        pytest.param(FileRequest(num_files=100, small=True), id="100 Small Files"),
        pytest.param(FileRequest(num_files=1, small=False), id="1 Large File"),
        pytest.param(FileRequest(num_files=20, small=False), id="20 Large Files"),
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
        pytest.param(FileRequest(num_files=1, small=True), 1024, id="1 Small File"),
        pytest.param(FileRequest(num_files=100, small=True), 102400, id="100 Small Files"),
        # pytest.param(FileRequest(num_files=1, small=False), id="1 Large File"),
        # pytest.param(FileRequest(num_files=20, small=False), id="20 Large Files"),
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
        pytest.param(FileRequest(num_files=1, small=True), id="1 Small File"),
        pytest.param(FileRequest(num_files=100, small=True), id="100 Small Files"),
        pytest.param(FileRequest(num_files=1, small=False), id="1 Large File"),
        pytest.param(FileRequest(num_files=20, small=False), id="20 Large Files"),
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
