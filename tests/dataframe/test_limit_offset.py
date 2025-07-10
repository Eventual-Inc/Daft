from __future__ import annotations

import pandas as pd
import pytest

import daft
from daft import DataFrame, col


@pytest.fixture(scope="session")
def tmp_data(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("test_limit_offset")
    items = [{"id": i, "name": f"user_{i}", "email": f"user_{i}@getdaft.io"} for i in range(1024)]

    print(f"Generating parquet data for Limit & Offset test, len: {len(items)}, path: {tmp_path}")
    daft.from_pylist(items).write_parquet(root_dir=str(tmp_path))
    return str(tmp_path)


def assert_result_num_rows(df: DataFrame, expected_num_rows: int) -> pd.DataFrame:
    pdf = df.to_pandas()
    assert len(pdf) == expected_num_rows, f"Expected {expected_num_rows} rows, got {len(pdf)}"
    return pdf


def test_negative_limit(tmp_data):
    df = daft.read_parquet(path=tmp_data)

    with pytest.raises(ValueError) as excinfo:
        df.select("name").limit(-1)
    assert "LIMIT <n> must be greater than or equal to 0, instead got: -1" in str(excinfo.value)


def test_limit(tmp_data):
    df = daft.read_parquet(path=tmp_data)

    df0 = df.select("name").limit(1)
    assert_result_num_rows(df0, 1)

    df1 = df.select("name").limit(1024)
    assert_result_num_rows(df1, 1024)

    df2 = df.select("name").limit(9223372036854775807)
    assert_result_num_rows(df2, 1024)


def test_limit_with_sort(tmp_data):
    df = daft.read_parquet(path=tmp_data)

    df0 = df.sort(by="id").select("name").limit(1)
    pdf = assert_result_num_rows(df0, 1)
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 1)], name="name"), check_names=True, check_index=False
    )

    df1 = df.select("id", "name").sort(by="id", desc=True).limit(1024)
    pdf = assert_result_num_rows(df1, 1024)
    pd.testing.assert_series_equal(
        pdf["name"],
        pd.Series([f"user_{i}" for i in range(1023, -1, -1)], name="name"),
        check_names=True,
        check_index=False,
    )

    df2 = df.sort(by="id").select("name").limit(9223372036854775807)
    pdf = assert_result_num_rows(df2, 1024)
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 1024)], name="name"), check_names=True, check_index=False
    )

    df3 = (
        df.select("id", "name")
        .limit(1024)
        .sort(by=col("id"), desc=False)
        .limit(17)
        .select("name")
        .limit(3)
        .select("name")
    )
    pdf = assert_result_num_rows(df3, 3)
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 3)], name="name"), check_names=True, check_index=False
    )


def test_negative_offset(tmp_data):
    df = daft.read_parquet(path=tmp_data)

    with pytest.raises(ValueError) as excinfo:
        df.select("name").offset(-1)
    assert "OFFSET <n> must be greater than or equal to 0, instead got: -1" in str(excinfo.value)


def test_offset(tmp_data):
    df = daft.read_parquet(path=tmp_data)

    df0 = df.select("name").offset(0)
    assert_result_num_rows(df0, 1024)

    df1 = df.select("name").offset(1024)
    assert_result_num_rows(df1, 0)

    df2 = df.select("name").offset(1023)
    assert_result_num_rows(df2, 1)

    df3 = df.select("name").offset(24)
    assert_result_num_rows(df3, 1000)

    df4 = df.select("name").offset(1025)
    assert_result_num_rows(df4, 0)

    df5 = df.select("name").offset(9223372036854775807)
    assert_result_num_rows(df5, 0)


def test_limit_offset(tmp_data):
    df = daft.read_parquet(path=tmp_data)

    df0 = df.select("name").limit(7).offset(0)
    assert_result_num_rows(df0, 7)

    df1 = df.select("name").limit(0).offset(7)
    assert_result_num_rows(df1, 0)

    df2 = df.select("name").limit(7).offset(2)
    assert_result_num_rows(df2, 5)

    df3 = df.select("name").limit(7).offset(7)
    assert_result_num_rows(df3, 0)

    # 0..7 -> 1..7 -> 1..6 -> 3..6
    df4 = df.limit(7).offset(1).select("name").limit(5).offset(2).limit(1024)
    assert_result_num_rows(df4, 3)

    # 0..17 -> 13..17 -> 13..16
    df5 = df.select("id", "name").limit(1024).limit(17).offset(5).select("name").offset(2).offset(6).limit(3)
    assert_result_num_rows(df5, 3)

    # 516..1024 -> 516..519
    df6 = df.select("id", "name").limit(1024)
    for i in range(1, 517):
        df6 = df6.offset(1).select("name")
    df6 = df6.limit(3)
    assert_result_num_rows(df6, 3)


def test_offset_limit(tmp_data):
    df = daft.read_parquet(path=tmp_data)

    df0 = df.select("name").offset(2).limit(0)
    assert_result_num_rows(df0, 0)

    df1 = df.select("name").offset(0).limit(7)
    assert_result_num_rows(df1, 7)

    df2 = df.select("name").offset(2).limit(7)
    assert_result_num_rows(df2, 7)

    df3 = df.select("name").offset(7).limit(7)
    assert_result_num_rows(df3, 7)

    # 7..24 -> 12..23 -> 19..23
    df4 = df.select("id", "name").offset(7).limit(17).select("name").offset(5).limit(11).offset(7)
    assert_result_num_rows(df4, 4)

    # 24..30 -> 27..30
    df5 = (
        df.offset(7)
        .select("id", "name")
        .offset(17)
        .limit(15)
        .limit(12)
        .select("name")
        .limit(6)
        .offset(3)
        .select("name")
    )
    assert_result_num_rows(df5, 3)

    # 516..522 -> 519..522
    df6 = df.select("id", "name")
    for i in range(1, 517):
        df6 = df6.offset(1).select("name")
    df6 = df6.limit(15).limit(12).limit(6).select("name").offset(3).select("name")
    assert_result_num_rows(df6, 3)


def test_limit_offset_with_sort(tmp_data):
    df = daft.read_parquet(path=tmp_data)

    df0 = df.select("id", "name").sort(by=col("id"), desc=False).limit(7).offset(0)
    pdf = assert_result_num_rows(df0, 7)
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 7)], name="name"), check_names=True, check_index=False
    )

    df1 = df.select("id", "name").sort(by=col("id"), desc=True).limit(0).offset(7)
    assert_result_num_rows(df1, 0)

    # 1023...1016 -> 1021...1016
    df2 = df.select("id", "name").sort(by=col("id"), desc=True).limit(7).offset(2)
    pdf = assert_result_num_rows(df2, 5)
    pd.testing.assert_series_equal(
        pdf["name"],
        pd.Series([f"user_{i}" for i in range(1021, 1016, -1)], name="name"),
        check_names=True,
        check_index=False,
    )

    df3 = df.select("id", "name").sort(by=col("id"), desc=False).limit(7).offset(7)
    assert_result_num_rows(df3, 0)

    # 0..7 -> 1..7 -> 1..6 -> 3..6
    df4 = df.select("id", "name").sort(by=col("id"), desc=False).limit(7).offset(1).limit(5).offset(2).limit(1024)
    pdf = assert_result_num_rows(df4, 3)
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(3, 6)], name="name"), check_names=True, check_index=False
    )

    # 0..17 -> 13..17 -> 13..16
    df5 = (
        df.select("id", "name")
        .limit(1024)
        .sort(by=col("id"), desc=False)
        .limit(17)
        .offset(5)
        .select("name")
        .offset(2)
        .offset(6)
        .limit(3)
        .select("name")
    )
    pdf = assert_result_num_rows(df5, 3)
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(13, 16)], name="name"), check_names=True, check_index=False
    )

    # 1023..0 -> 507..0 -> 507..504
    df6 = df.select("id", "name").sort(by="id", desc=True).limit(1024)
    for i in range(1, 517):
        df6 = df6.offset(1).select("name")
    df6 = df6.limit(3)
    pdf = assert_result_num_rows(df6, 3)
    pd.testing.assert_series_equal(
        pdf["name"],
        pd.Series([f"user_{i}" for i in range(507, 504, -1)], name="name"),
        check_names=True,
        check_index=False,
    )


def test_offset_limit_with_sort(tmp_data):
    df = daft.read_parquet(path=tmp_data)

    df0 = df.select("id", "name").sort(by=col("id"), desc=False).offset(2).limit(0)
    assert_result_num_rows(df0, 0)

    df1 = df.select("id", "name").sort(by=col("id"), desc=False).offset(0).limit(7)
    pdf = assert_result_num_rows(df1, 7)
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 7)], name="name"), check_names=True, check_index=False
    )

    # 1023..0 -> 1021..0 -> 1021..1014
    df2 = df.select("id", "name").sort(by=col("id"), desc=True).offset(2).limit(7)
    pdf = assert_result_num_rows(df2, 7)
    pd.testing.assert_series_equal(
        pdf["name"],
        pd.Series([f"user_{i}" for i in range(1021, 1014, -1)], name="name"),
        check_names=True,
        check_index=False,
    )

    df3 = df.select("id", "name").sort(by=col("id"), desc=False).offset(7).limit(7)
    pdf = assert_result_num_rows(df3, 7)
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(7, 14)], name="name"), check_names=True, check_index=False
    )

    # 1016..0 -> 1016..999 -> 1011..1000 -> 1004..1000
    df4 = df.select("id", "name").sort(by=col("id"), desc=True).offset(7).limit(17).offset(5).limit(11).offset(7)
    pdf = assert_result_num_rows(df4, 4)
    pd.testing.assert_series_equal(
        pdf["name"],
        pd.Series([f"user_{i}" for i in range(1004, 1000, -1)], name="name"),
        check_names=True,
        check_index=False,
    )

    # 1023..0 -> 999..993 -> 996..993
    df5 = (
        df.select("id", "name")
        .limit(1024)
        .sort(by=col("id"), desc=True)
        .select("name")
        .offset(7)
        .select("name")
        .offset(17)
        .select("name")
        .limit(15)
        .select("name")
        .limit(12)
        .select("name")
        .limit(6)
        .offset(3)
        .select("name")
    )
    pdf = assert_result_num_rows(df5, 3)
    pd.testing.assert_series_equal(
        pdf["name"],
        pd.Series([f"user_{i}" for i in range(996, 993, -1)], name="name"),
        check_names=True,
        check_index=False,
    )

    # 516..522 -> 519..522
    df6 = df.select("id", "name").sort(by=col("id"), desc=False)
    for i in range(1, 517):
        df6 = df6.offset(1).select("name")
    df6 = df6.limit(15).limit(12).limit(6).offset(3)
    pdf = assert_result_num_rows(df6, 3)
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(519, 522)], name="name"), check_names=True, check_index=False
    )
