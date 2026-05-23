from __future__ import annotations

import os
import urllib.parse
import uuid
from datetime import date, datetime

import pyarrow as pa
import pytest
from pyarrow import dataset as pads

import daft
from tests.conftest import assert_df_equals, get_tests_daft_runner_name
from tests.cookbook.assets import COOKBOOK_DATA_CSV


def test_parquet_write(tmp_path, with_morsel_size):
    df = daft.read_csv(COOKBOOK_DATA_CSV)

    pd_df = df.write_parquet(tmp_path)
    read_back_pd_df = daft.read_parquet(tmp_path.as_posix() + "/*.parquet").to_pandas()
    assert_df_equals(df.to_pandas(), read_back_pd_df)

    assert len(pd_df) == 1
    assert pd_df._preview.partition is None
    pd_df.__repr__()
    assert len(pd_df._preview.partition) == 1


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native",
    reason="single_file is only supported on the native runner",
)
def test_parquet_write_single_file(tmp_path, with_morsel_size):
    df = daft.read_csv(COOKBOOK_DATA_CSV)

    out_path = tmp_path / "out.parquet"
    result = df.write_parquet(out_path.as_posix(), single_file=True)

    assert out_path.is_file()
    assert result.to_pydict()["path"] == [out_path.as_posix()]

    read_back = daft.read_parquet(out_path.as_posix()).to_pandas()
    assert_df_equals(df.to_pandas(), read_back)


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native",
    reason="single_file is only supported on the native runner",
)
def test_parquet_write_single_file_creates_parent_dirs(tmp_path, with_morsel_size):
    df = daft.from_pydict({"x": [1, 2, 3]})
    out_path = tmp_path / "nested" / "subdir" / "out.parquet"

    df.write_parquet(out_path.as_posix(), single_file=True)

    assert out_path.is_file()
    assert daft.read_parquet(out_path.as_posix()).to_pydict() == {"x": [1, 2, 3]}


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native",
    reason="single_file is only supported on the native runner",
)
def test_parquet_write_single_file_large_does_not_split(tmp_path, with_morsel_size):
    # Data bigger than default target_filesize should still produce a single file.
    rows = 100_000
    df = daft.from_pydict({"x": list(range(rows)), "y": ["hello" * 100] * rows})

    out_path = tmp_path / "big.parquet"
    df.write_parquet(out_path.as_posix(), single_file=True)

    assert out_path.is_file()
    assert daft.read_parquet(out_path.as_posix()).count_rows() == rows


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native",
    reason="single_file is only supported on the native runner",
)
def test_parquet_write_single_file_empty(tmp_path, with_morsel_size):
    # Empty DataFrame must not panic when single_file=True. CommitWriteSink emits an
    # empty parquet file at the exact path so reads round-trip.
    df = daft.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"]}).where(daft.lit(False))

    out_path = tmp_path / "empty.parquet"
    df.write_parquet(out_path.as_posix(), single_file=True)

    assert out_path.is_file()
    assert daft.read_parquet(out_path.as_posix()).count_rows() == 0


def test_parquet_write_single_file_rejects_partition_cols(tmp_path):
    df = daft.from_pydict({"x": [1, 2], "y": ["a", "b"]})
    with pytest.raises(ValueError, match="single_file=True.*partition_cols"):
        df.write_parquet(
            (tmp_path / "out.parquet").as_posix(),
            single_file=True,
            partition_cols=["y"],
        )


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native",
    reason="single_file is only supported on the native runner",
)
def test_parquet_write_single_file_overwrite(tmp_path, with_morsel_size):
    out_path = tmp_path / "out.parquet"
    daft.from_pydict({"x": [1, 2, 3]}).write_parquet(out_path.as_posix(), single_file=True)
    daft.from_pydict({"x": [10, 20, 30, 40, 50]}).write_parquet(
        out_path.as_posix(), single_file=True, write_mode="overwrite"
    )
    assert daft.read_parquet(out_path.as_posix()).to_pydict() == {"x": [10, 20, 30, 40, 50]}


def test_parquet_write_single_file_rejects_write_success_file(tmp_path):
    df = daft.from_pydict({"x": [1, 2]})
    with pytest.raises(ValueError, match="single_file=True.*write_success_file"):
        df.write_parquet(
            (tmp_path / "out.parquet").as_posix(),
            single_file=True,
            write_success_file=True,
        )


def test_parquet_write_with_partitioning(tmp_path, with_morsel_size):
    df = daft.read_csv(COOKBOOK_DATA_CSV)

    pd_df = df.write_parquet(tmp_path, partition_cols=["Borough"])

    read_back_pd_df = daft.read_parquet(tmp_path.as_posix() + "/**/*.parquet", hive_partitioning=True).to_pandas()
    assert_df_equals(df.to_pandas(), read_back_pd_df)

    assert len(pd_df) == 5
    assert pd_df._preview.partition is None
    pd_df.__repr__()
    assert len(pd_df._preview.partition) == 5


@pytest.mark.parametrize("write_mode", ["append", "overwrite"])
def test_empty_parquet_write_without_partitioning(tmp_path, write_mode, with_morsel_size):
    df = daft.read_csv(COOKBOOK_DATA_CSV)
    df = df.where(daft.lit(False))

    # Create a unique path to make sure that the writer is comfortable with nonexistent directories
    path = os.path.join(tmp_path, str(uuid.uuid4()))
    pd_df = df.write_parquet(path, write_mode=write_mode)
    read_back_pd_df = daft.read_parquet(path).to_pandas()
    assert_df_equals(df.to_pandas(), read_back_pd_df)

    assert len(pd_df) == 1
    assert pd_df._preview.partition is None
    pd_df.__repr__()
    assert len(pd_df._preview.partition) == 1


@pytest.mark.parametrize("write_mode", ["append", "overwrite"])
def test_empty_parquet_write_with_partitioning(tmp_path, write_mode, with_morsel_size):
    df = daft.read_csv(COOKBOOK_DATA_CSV)
    df = df.where(daft.lit(False))

    # Create a unique path to make sure that the writer is comfortable with nonexistent directories
    path = os.path.join(tmp_path, str(uuid.uuid4()))
    output_files = df.write_parquet(path, partition_cols=["Borough"], write_mode=write_mode)
    assert len(output_files) == 0

    with pytest.raises(FileNotFoundError):
        daft.read_parquet(os.path.join(path, "**/*.parquet")).to_pandas()


def test_parquet_write_with_partitioning_readback_values(tmp_path, with_morsel_size):
    df = daft.read_csv(COOKBOOK_DATA_CSV)

    output_files = df.write_parquet(tmp_path, partition_cols=["Borough"])
    output_dict = output_files.to_pydict()
    boroughs = {"QUEENS", "MANHATTAN", "STATEN ISLAND", "BROOKLYN", "BRONX"}
    assert set(output_dict["Borough"]) == boroughs

    for path, bor in zip(output_dict["path"], output_dict["Borough"]):
        assert f"Borough={urllib.parse.quote(bor)}" in path
        read_back = daft.read_parquet(path, hive_partitioning=True).to_pydict()
        assert all(b == bor for b in read_back["Borough"])

    read_back_pd_df = daft.read_parquet(tmp_path.as_posix() + "/**/*.parquet", hive_partitioning=True).to_pandas()
    assert_df_equals(df.to_pandas(), read_back_pd_df)

    assert len(output_files) == 5
    assert output_files._preview.partition is None
    output_files.__repr__()
    assert len(output_files._preview.partition) == 5


@pytest.mark.parametrize(
    "exp,key,answer",
    [
        (
            daft.col("date").partition_days(),
            "date_days",
            [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1), date(2024, 4, 1), date(2024, 5, 1)],
        ),
        (daft.col("date").partition_hours(), "date_hours", [473352, 474096, 474792, 475536, 476256]),
        (daft.col("date").partition_months(), "date_months", [648, 649, 650, 651, 652]),
        (daft.col("date").partition_years(), "date_years", [54]),
    ],
)
def test_parquet_write_with_iceberg_date_partitioning(exp, key, answer, tmp_path, with_morsel_size):
    data = {
        "id": [1, 2, 3, 4, 5],
        "date": [
            datetime(2024, 1, 1),
            datetime(2024, 2, 1),
            datetime(2024, 3, 1),
            datetime(2024, 4, 1),
            datetime(2024, 5, 1),
        ],
    }
    df = daft.from_pydict(data)
    date_files = df.write_parquet(tmp_path, partition_cols=[exp]).sort(by=key)
    output_dict = date_files.to_pydict()
    assert len(output_dict[key]) == len(answer)
    assert output_dict[key] == answer
    read_back_pd_df = daft.read_parquet(tmp_path.as_posix() + "/**/*.parquet").to_pandas()
    assert_df_equals(df.to_pandas(), read_back_pd_df, sort_key="id")


@pytest.mark.parametrize(
    "exp,key,answer",
    [
        (daft.col("id").partition_iceberg_bucket(10), "id_bucket", [0, 3, 5, 6, 8]),
        (daft.col("id").partition_iceberg_truncate(10), "id_trunc", [0, 10, 20, 40]),
    ],
)
def test_parquet_write_with_iceberg_bucket_and_trunc(exp, key, answer, tmp_path, with_morsel_size):
    data = {
        "id": [1, 12, 23, 24, 45],
        "date": [
            datetime(2024, 1, 1),
            datetime(2024, 2, 1),
            datetime(2024, 3, 1),
            datetime(2024, 4, 1),
            datetime(2024, 5, 1),
        ],
    }
    df = daft.from_pydict(data)
    date_files = df.write_parquet(tmp_path, partition_cols=[exp]).sort(by=key)
    output_dict = date_files.to_pydict()
    assert len(output_dict[key]) == len(answer)
    assert output_dict[key] == answer
    read_back_pd_df = daft.read_parquet(tmp_path.as_posix() + "/**/*.parquet").to_pandas()
    assert_df_equals(df.to_pandas(), read_back_pd_df, sort_key="id")


def test_parquet_write_with_null_values(tmp_path, with_morsel_size):
    df = daft.from_pydict({"x": [1, 2, 3, None]})
    df.write_parquet(tmp_path, partition_cols=[df["x"].alias("y")])
    ds = pads.dataset(tmp_path, format="parquet", partitioning=pads.HivePartitioning(pa.schema([("y", pa.int64())])))
    readback = ds.to_table()
    assert readback.to_pydict() == {"x": [1, 2, 3, None], "y": [1, 2, 3, None]}


@pytest.fixture()
def smaller_parquet_target_filesize():
    with daft.execution_config_ctx(parquet_target_filesize=1024):
        yield


def test_parquet_write_multifile(tmp_path, smaller_parquet_target_filesize, with_morsel_size):
    data = {"x": list(range(1_000))}
    df = daft.from_pydict(data)
    df2 = df.write_parquet(tmp_path)
    assert len(df2) > 1
    read_back = daft.read_parquet(tmp_path.as_posix() + "/*.parquet").sort(by="x").to_pydict()
    assert read_back == data


def test_parquet_write_multifile_with_partitioning(tmp_path, smaller_parquet_target_filesize, with_morsel_size):
    data = {"x": list(range(1_000))}
    df = daft.from_pydict(data)
    df2 = df.write_parquet(tmp_path, partition_cols=[df["x"].alias("y") % 2])
    assert len(df2) >= 4
    ds = pads.dataset(tmp_path, format="parquet", partitioning=pads.HivePartitioning(pa.schema([("y", pa.int64())])))
    readback = ds.to_table()
    readback = readback.sort_by("x").to_pydict()
    assert readback["x"] == data["x"]
    assert readback["y"] == [y % 2 for y in data["x"]]


def test_parquet_write_with_some_empty_partitions(tmp_path, with_morsel_size):
    data = {"x": [1, 2, 3], "y": ["a", "b", "c"]}
    daft.from_pydict(data).into_partitions(4).write_parquet(tmp_path)

    read_back = daft.read_parquet(tmp_path.as_posix() + "/**/*.parquet").sort("x").to_pydict()
    assert read_back == data


def test_parquet_partitioned_write_with_some_empty_partitions(tmp_path, with_morsel_size):
    data = {"x": [1, 2, 3], "y": ["a", "b", "c"]}
    output_files = daft.from_pydict(data).into_partitions(4).write_parquet(tmp_path, partition_cols=["x"])

    assert len(output_files) == 3

    read_back = daft.read_parquet(tmp_path.as_posix() + "/**/*.parquet", hive_partitioning=True).sort("x").to_pydict()
    assert read_back == data


def test_csv_write(tmp_path, with_morsel_size):
    df = daft.read_csv(COOKBOOK_DATA_CSV)

    pd_df = df.write_csv(tmp_path)

    read_back_pd_df = daft.read_csv(tmp_path.as_posix() + "/*.csv").to_pandas()
    assert_df_equals(df.to_pandas(), read_back_pd_df)

    assert len(pd_df) == 1
    assert pd_df._preview.partition is None
    pd_df.__repr__()
    assert len(pd_df._preview.partition) == 1


def test_csv_write_with_partitioning(tmp_path, with_morsel_size):
    df = daft.read_csv(COOKBOOK_DATA_CSV)
    schema = df.schema()
    names = schema.column_names()
    types = {}
    for n in names:
        types[n] = schema[n].dtype

    pd_df = df.write_csv(tmp_path, partition_cols=["Borough"]).to_pandas()
    read_back_pd_df = daft.read_csv(tmp_path.as_posix() + "/**/*.csv", schema=types, hive_partitioning=True).to_pandas()
    assert_df_equals(df.to_pandas().fillna(""), read_back_pd_df.fillna(""))

    assert len(pd_df) == 5


def test_empty_csv_write(tmp_path, with_morsel_size):
    df = daft.read_csv(COOKBOOK_DATA_CSV)
    df = df.where(daft.lit(False))

    schema = df.schema()
    names = schema.column_names()
    types = {}
    for n in names:
        types[n] = schema[n].dtype

    pd_df = df.write_csv(tmp_path)
    read_back_pd_df = daft.read_csv(tmp_path.as_posix() + "/*.csv", schema=types).to_pandas()
    assert_df_equals(df.to_pandas().fillna(""), read_back_pd_df.fillna(""))

    assert len(pd_df) == 1
    assert pd_df._preview.partition is None
    pd_df.__repr__()
    assert len(pd_df._preview.partition) == 1


def test_empty_csv_write_with_partitioning(tmp_path, with_morsel_size):
    df = daft.read_csv(COOKBOOK_DATA_CSV)
    df = df.where(daft.lit(False))

    schema = df.schema()
    names = schema.column_names()
    types = {}
    for n in names:
        types[n] = schema[n].dtype

    pd_df = df.write_csv(tmp_path, partition_cols=["Borough"])
    assert len(pd_df) == 0
    with pytest.raises(FileNotFoundError):
        daft.read_csv(tmp_path.as_posix() + "/**/*.csv", schema=types).to_pandas()


def test_csv_write_with_some_empty_partitions(tmp_path, with_morsel_size):
    data = {"x": [1, 2, 3], "y": ["a", "b", "c"]}
    daft.from_pydict(data).into_partitions(4).write_csv(tmp_path)

    read_back = daft.read_csv(tmp_path.as_posix() + "/**/*.csv").sort("x").to_pydict()
    assert read_back == data


def test_csv_partitioned_write_with_some_empty_partitions(tmp_path, with_morsel_size):
    data = {"x": [1, 2, 3], "y": ["a", "b", "c"]}
    output_files = daft.from_pydict(data).into_partitions(4).write_csv(tmp_path, partition_cols=["x"])

    assert len(output_files) == 3

    read_back = daft.read_csv(tmp_path.as_posix() + "/**/*.csv", hive_partitioning=True).sort("x").to_pydict()
    assert read_back == data


@pytest.fixture()
def smaller_json_target_filesize():
    with daft.execution_config_ctx(json_target_filesize=1024):
        yield


def test_json_write_multifile(tmp_path, smaller_json_target_filesize):
    data = {"x": list(range(1_000))}
    df = daft.from_pydict(data)
    df2 = df.write_json(tmp_path)
    assert len(df2) > 1
    read_back = daft.read_json(tmp_path.as_posix() + "/*.json").sort(by="x").to_pydict()
    assert read_back == data


def test_json_write_multifile_with_partitioning(tmp_path, smaller_json_target_filesize):
    data = {"x": list(range(1_000))}
    df = daft.from_pydict(data)
    df2 = df.write_json(tmp_path, partition_cols=[df["x"].alias("y") % 2])
    assert len(df2) >= 4
    read_back = daft.read_json(tmp_path.as_posix() + "/**/*.json").sort(by="x").to_pydict()
    assert read_back["x"] == data["x"]
