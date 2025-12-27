from __future__ import annotations

import datetime
import decimal
import os
import re
import uuid

import pyarrow as pa
import pytest

import daft
from daft import DataType, TimeUnit
from daft.io import FilenameProvider
from tests.conftest import get_tests_daft_runner_name

from ..integration.io.conftest import minio_create_bucket

PYARROW_GE_11_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (11, 0, 0)


@pytest.mark.skipif(
    not PYARROW_GE_11_0_0,
    reason="PyArrow writing to CSV does not have good coverage for all types for versions <11.0.0",
)
@pytest.mark.parametrize(
    ["data", "pa_type", "expected_dtype", "expected_inferred_dtype"],
    [
        ([1, 2, None], pa.int64(), DataType.int64(), DataType.int64()),
        (["a", "b", ""], pa.large_string(), DataType.string(), DataType.string()),
        ([b"a", b"b", b""], pa.large_binary(), DataType.binary(), DataType.string()),
        ([True, False, None], pa.bool_(), DataType.bool(), DataType.bool()),
        ([None, None, None], pa.null(), DataType.null(), DataType.null()),
        (
            [decimal.Decimal("1.23"), decimal.Decimal("1.24"), None],
            pa.decimal128(16, 8),
            DataType.decimal128(16, 8),
            DataType.float64(),
        ),
        ([datetime.date(1994, 1, 1), datetime.date(1995, 1, 1), None], pa.date32(), DataType.date(), DataType.date()),
        (
            [datetime.time(1, 2, 3, 4), datetime.time(5, 6, 7, 8), None],
            pa.time64("us"),
            DataType.time(TimeUnit.us()),
            DataType.time(TimeUnit.us()),
        ),
        (
            [datetime.time(1, 2, 3, 4), datetime.time(5, 6, 7, 8), None],
            pa.time64("ns"),
            DataType.time(TimeUnit.ns()),
            DataType.time(TimeUnit.us()),
        ),
        (
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("ms"),
            DataType.timestamp(TimeUnit.ms()),
            # NOTE: Seems like the inferred type is seconds because it's written with seconds resolution
            DataType.timestamp(TimeUnit.s()),
        ),
        (
            [datetime.date(1994, 1, 1), datetime.date(1995, 1, 1), None],
            pa.date64(),
            DataType.timestamp(TimeUnit.ms()),
            DataType.timestamp(TimeUnit.s()),
        ),
        (
            [datetime.timedelta(days=1), datetime.timedelta(days=2), None],
            pa.duration("ms"),
            DataType.duration(TimeUnit.ms()),
            # NOTE: Duration ends up being written as int64
            DataType.int64(),
        ),
    ],
)
def test_roundtrip_simple_arrow_types(tmp_path, data, pa_type, expected_dtype, expected_inferred_dtype):
    before = daft.from_arrow(pa.table({"id": pa.array(range(3)), "foo": pa.array(data, type=pa_type)}))
    before = before.concat(before)
    before.write_csv(str(tmp_path))
    after = daft.read_csv(str(tmp_path))
    assert before.schema()["foo"].dtype == expected_dtype
    assert after.schema()["foo"].dtype == expected_inferred_dtype
    assert before.to_arrow() == after.with_column("foo", after["foo"].cast(expected_dtype)).to_arrow()


@pytest.mark.skipif(
    not PYARROW_GE_11_0_0,
    reason="PyArrow writing to CSV does not have good coverage for all types for versions <11.0.0",
)
@pytest.mark.parametrize(
    ["data", "pa_type"],
    [
        ([[1, 2, 3], [], None], pa.large_list(pa.int64())),
        ([[1, 2, 3], [4, 5, 6], None], pa.list_(pa.int64(), list_size=3)),
        ([{"bar": 1}, {"bar": None}, None], pa.struct([("bar", pa.int64())])),
        ([{"k": 1}, {"k": None}, None], pa.map_(pa.large_string(), pa.int64())),
        ([b"\xff\xfe", b"\x80\x81", None], pa.large_binary()),
    ],
)
def test_write_csv_unsupported_types_raise(tmp_path, data, pa_type):
    with pytest.raises(Exception):
        before = daft.from_arrow(pa.table({"id": pa.array(range(3)), "foo": pa.array(data, type=pa_type)}))
        before.write_csv(str(tmp_path))


def test_write_and_read_empty_csv(tmp_path_factory):
    empty_csv_files = str(tmp_path_factory.mktemp("empty_csv"))
    df = daft.from_pydict({"a": []})
    df.write_csv(empty_csv_files, write_mode="overwrite")

    assert daft.read_csv(empty_csv_files).to_pydict() == {"a": []}


class RecordingBlockFilenameProvider(FilenameProvider):
    """FilenameProvider used to test CSV block-oriented writes.

    The implementation mirrors the parquet tests but uses the "csv" extension.
    """

    def __init__(self) -> None:  # pragma: no cover - exercised via higher-level IO tests
        self.calls: list[tuple[str, int, int, int, str]] = []

    def get_filename_for_block(
        self,
        write_uuid: str,
        task_index: int,
        block_index: int,
        file_idx: int,
        ext: str,
    ) -> str:
        self.calls.append((write_uuid, task_index, block_index, file_idx, ext))
        return f"csv-{write_uuid}-{task_index}-{block_index}-{file_idx}.{ext}"

    def get_filename_for_row(
        self,
        row: dict[str, object],
        write_uuid: str,
        task_index: int,
        block_index: int,
        row_index: int,
        ext: str,
    ) -> str:  # pragma: no cover - not used in these tests
        raise AssertionError("get_filename_for_row should not be called for block writes")


def _extract_basenames(paths: list[str]) -> list[str]:
    basenames: list[str] = []
    for path in paths:
        # Handle file:// prefix
        if path.startswith("file://"):
            path = path[len("file://") :]
        # Handle S3 paths by taking everything after the bucket
        elif "://" in path:
            path = path.split("://", 1)[1].split("/", 1)[1]
        basenames.append(os.path.basename(path))
    return basenames


def _check_filename_provider_results(basenames, prefix, extension, provider):
    assert basenames
    # Pattern: <prefix>-<uuid>-<task>-<block>-<file>.<extension>
    uuid_pattern = r"[0-9a-f]{32}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    pattern = re.compile(rf"{prefix}-({uuid_pattern})-\d+-\d+-\d+\.{extension}")

    matches = [pattern.match(name) for name in basenames]
    assert all(matches), f"Some filenames did not match the pattern: {basenames}"

    # Verify all files share the same write UUID
    uuids = {m.group(1) for m in matches if m}
    assert len(uuids) == 1

    # In non-distributed mode, we can also check the provider's internal call record
    if get_tests_daft_runner_name() != "ray":
        assert len(provider.calls) == len(basenames)
        assert {call[0] for call in provider.calls} == uuids
        assert {call[4] for call in provider.calls} == {extension}


def test_filename_provider_csv_local(tmp_path) -> None:
    data = {"a": [1, 2, 3], "b": ["x", "y", "z"]}
    df = daft.from_pydict(data).repartition(2)

    provider = RecordingBlockFilenameProvider()
    result_df = df.write_csv(str(tmp_path), filename_provider=provider)
    basenames = _extract_basenames(result_df.to_pydict()["path"])
    _check_filename_provider_results(basenames, "csv", "csv", provider)


@pytest.mark.integration()
def test_filename_provider_csv_s3(minio_io_config) -> None:
    bucket_name = "filename-provider-csv"
    data = {"a": [1, 2, 3], "b": ["x", "y", "z"]}

    provider = RecordingBlockFilenameProvider()

    with minio_create_bucket(minio_io_config=minio_io_config, bucket_name=bucket_name):
        result_df = (
            daft.from_pydict(data)
            .repartition(2)
            .write_csv(
                f"s3://{bucket_name}/csv-writes-{uuid.uuid4()!s}",
                partition_cols=["b"],
                io_config=minio_io_config,
                filename_provider=provider,
            )
        )
        basenames = _extract_basenames(result_df.to_pydict()["path"])
        _check_filename_provider_results(basenames, "csv", "csv", provider)
