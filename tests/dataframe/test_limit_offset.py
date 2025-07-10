from __future__ import annotations

import random

import pandas as pd
import pytest

import daft
from daft import col


# TODO remove it by zhenchao 2025-07-08 17:57:21
def test():
    df = daft.from_pydict({"id": [i for i in range(1024)], "name": [f"user_{i}" for i in range(1024)]})

    # 7..17 -> 10..17 -> 10..15 -> 11..15
    df2 = df.select("id", "name").limit(0).filter(col("id") >= 10).select("name").limit(5).offset(1)
    df2.explain(show_all=True)
    pdf = df2.to_pandas()
    print(pdf.count())
    print(pdf)
    # assert len(pdf) == 7, f"Expected 7 rows, got {len(pdf)}"
    # pd.testing.assert_series_equal(
    #     pdf["name"], pd.Series([f"user_{i}" for i in range(2, 9)], name="name"), check_names=True, check_index=False
    # )


def test_limit():
    df = daft.from_pydict({"id": [i for i in range(1024)], "name": [f"user_{i}" for i in range(1024)]})

    df0 = df.select("name").limit(1)
    pdf = df0.to_pandas()

    assert len(pdf) == 1, f"Expected 1 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 1)], name="name"), check_names=True, check_index=False
    )

    df1 = df.select("name").limit(1024)
    pdf = df1.to_pandas()

    assert len(pdf) == 1024, f"Expected 1024 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 1024)], name="name"), check_names=True, check_index=False
    )

    df2 = df.select("name").limit(9223372036854775807)
    pdf = df2.to_pandas()

    assert len(pdf) == 1024, f"Expected 1024 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 1024)], name="name"), check_names=True, check_index=False
    )


def test_negative_limit():
    df = daft.from_pydict({"id": [i for i in range(1024)], "name": [f"user_{i}" for i in range(1024)]})

    with pytest.raises(ValueError) as excinfo:
        df.select("name").limit(-1)

    assert "LIMIT <n> must be greater than or equal to 0, instead got: -1" in str(excinfo.value)


def test_offset():
    df = daft.from_pydict({"id": [i for i in range(1024)], "name": [f"user_{i}" for i in range(1024)]})

    df0 = df.select("name").offset(0)
    pdf = df0.to_pandas()
    assert len(pdf) == 1024, f"Expected 1024 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 1024)], name="name"), check_names=True, check_index=False
    )

    df1 = df.select("name").offset(1024)
    pdf = df1.to_pandas()
    assert len(pdf) == 0, f"Expected 0 rows, got {len(pdf)}"

    df2 = df.select("name").offset(1023)
    pdf = df2.to_pandas()
    assert len(pdf) == 1, f"Expected 1 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"],
        pd.Series([f"user_{i}" for i in range(1023, 1024)], name="name"),
        check_names=True,
        check_index=False,
    )

    df3 = df.select("name").offset(24)
    pdf = df3.to_pandas()
    assert len(pdf) == 1000, f"Expected 1000 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(24, 1024)], name="name"), check_names=True, check_index=False
    )

    df4 = df.select("name").offset(1025)
    pdf = df4.to_pandas()
    assert len(pdf) == 0, f"Expected 0 rows, got {len(pdf)}"

    df5 = df.select("name").offset(9223372036854775807)
    pdf = df5.to_pandas()
    assert len(pdf) == 0, f"Expected 0 rows, got {len(pdf)}"


def test_negative_offset():
    df = daft.from_pydict({"id": [i for i in range(1024)], "name": [f"user_{i}" for i in range(1024)]})

    with pytest.raises(ValueError) as excinfo:
        df.select("name").offset(-1)

    assert "OFFSET <n> must be greater than or equal to 0, instead got: -1" in str(excinfo.value)


def test_limit_offset():
    df = daft.from_pydict({"id": [i for i in range(1024)], "name": [f"user_{i}" for i in range(1024)]})

    df0 = df.select("name").limit(7).offset(0)
    pdf = df0.to_pandas()
    assert len(pdf) == 7, f"Expected 7 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 7)], name="name"), check_names=True, check_index=False
    )

    df1 = df.select("name").limit(0).offset(7)
    pdf = df1.to_pandas()
    assert len(pdf) == 0, f"Expected 0 rows, got {len(pdf)}"

    df2 = df.select("name").limit(7).offset(2)
    pdf = df2.to_pandas()
    assert len(pdf) == 5, f"Expected 5 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(2, 7)], name="name"), check_names=True, check_index=False
    )

    df3 = df.select("name").limit(7).offset(7)
    pdf = df3.to_pandas()
    assert len(pdf) == 0, f"Expected 0 rows, got {len(pdf)}"

    # 0..6 -> 1..6 -> 1..5 -> 3..5
    df4 = df.select("name").limit(7).offset(1).limit(5).offset(2).limit(1024)
    pdf = df4.to_pandas()
    assert len(pdf) == 3, f"Expected 3 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(3, 6)], name="name"), check_names=True, check_index=False
    )

    # 0..17 -> 13..17 -> 13..15
    df5 = df.select("name").limit(1024).limit(17).offset(5).offset(2).offset(6).limit(3)
    pdf = df5.to_pandas()
    assert len(pdf) == 3, f"Expected 3 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(13, 16)], name="name"), check_names=True, check_index=False
    )

    # 516..1024 -> 516..519
    df6 = df.select("name").limit(1024)
    for i in range(1, 517):
        df6 = df6.offset(1)
    df6 = df6.limit(3)
    pdf = df6.to_pandas()
    assert len(pdf) == 3, f"Expected 3 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(516, 519)], name="name"), check_names=True, check_index=False
    )


def test_offset_limit():
    df = daft.from_pydict({"id": [i for i in range(1024)], "name": [f"user_{i}" for i in range(1024)]})

    df0 = df.select("name").offset(2).limit(0)
    pdf = df0.to_pandas()
    assert len(pdf) == 0, f"Expected 0 rows, got {len(pdf)}"

    df1 = df.select("name").offset(0).limit(7)
    pdf = df1.to_pandas()
    assert len(pdf) == 7, f"Expected 7 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 7)], name="name"), check_names=True, check_index=False
    )

    df2 = df.select("name").offset(2).limit(7)
    pdf = df2.to_pandas()
    assert len(pdf) == 7, f"Expected 7 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(2, 9)], name="name"), check_names=True, check_index=False
    )

    df3 = df.select("name").offset(7).limit(7)
    pdf = df3.to_pandas()
    assert len(pdf) == 7, f"Expected 7 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(7, 14)], name="name"), check_names=True, check_index=False
    )

    # 7..24 -> 12..24 -> 12..23 -> 19..23
    df4 = df.select("name").offset(7).limit(17).offset(5).limit(11).offset(7)
    pdf = df4.to_pandas()
    assert len(pdf) == 4, f"Expected 5 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(19, 23)], name="name"), check_names=True, check_index=False
    )

    # 24..30 -> 27..30
    df5 = df.select("name").offset(7).offset(17).limit(15).limit(12).limit(6).offset(3)
    pdf = df5.to_pandas()
    assert len(pdf) == 3, f"Expected 3 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(27, 30)], name="name"), check_names=True, check_index=False
    )

    # 516..522 -> 519..522
    df6 = df.select("name")
    for i in range(1, 517):
        df6 = df6.offset(1)
    df6 = df6.limit(15).limit(12).limit(6).offset(3)
    pdf = df6.to_pandas()
    assert len(pdf) == 3, f"Expected 3 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(519, 522)], name="name"), check_names=True, check_index=False
    )


def test_limit_offset_with_sort():
    uids = [i for i in range(1024)]
    random.shuffle(uids)
    users = {"id": [], "name": []}
    for uid in uids:
        users["id"].append(uid)
        users["name"].append(f"user_{uid}")

    df = daft.from_pydict(users)

    df0 = df.select("id", "name").sort(by=col("id"), desc=False).limit(7).offset(0)
    pdf = df0.to_pandas()
    assert len(pdf) == 7, f"Expected 7 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 7)], name="name"), check_names=True, check_index=False
    )

    df1 = df.select("id", "name").sort(by=col("id"), desc=True).limit(0).offset(7)
    pdf = df1.to_pandas()
    assert len(pdf) == 0, f"Expected 0 rows, got {len(pdf)}"

    # 1023...1016 -> 1021...1016
    df2 = df.select("id", "name").sort(by=col("id"), desc=True).limit(7).offset(2)
    pdf = df2.to_pandas()
    assert len(pdf) == 5, f"Expected 5 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"],
        pd.Series([f"user_{i}" for i in range(1021, 1016, -1)], name="name"),
        check_names=True,
        check_index=False,
    )

    df3 = df.select("id", "name").sort(by=col("id"), desc=False).limit(7).offset(7)
    pdf = df3.to_pandas()
    assert len(pdf) == 0, f"Expected 0 rows, got {len(pdf)}"

    # 0..6 -> 1..6 -> 1..5 -> 3..5
    df4 = df.select("id", "name").sort(by=col("id"), desc=False).limit(7).offset(1).limit(5).offset(2).limit(1024)
    pdf = df4.to_pandas()
    assert len(pdf) == 3, f"Expected 3 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(3, 6)], name="name"), check_names=True, check_index=False
    )

    # 0..17 -> 13..17 -> 13..15
    df5 = (
        df.select("id", "name")
        .sort(by=col("id"), desc=False)
        .limit(1024)
        .limit(17)
        .offset(5)
        .offset(2)
        .offset(6)
        .limit(3)
    )
    pdf = df5.to_pandas()
    assert len(pdf) == 3, f"Expected 3 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(13, 16)], name="name"), check_names=True, check_index=False
    )

    # 1023..0 -> 507..0 -> 507..504
    df6 = df.select("id", "name").sort(by="id", desc=True).limit(1024)
    for i in range(1, 517):
        df6 = df6.offset(1)
    df6 = df6.limit(3)
    pdf = df6.to_pandas()
    assert len(pdf) == 3, f"Expected 3 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"],
        pd.Series([f"user_{i}" for i in range(507, 504, -1)], name="name"),
        check_names=True,
        check_index=False,
    )


def test_offset_limit_with_sort():
    uids = [i for i in range(1024)]
    random.shuffle(uids)
    users = {"id": [], "name": []}
    for uid in uids:
        users["id"].append(uid)
        users["name"].append(f"user_{uid}")

    df = daft.from_pydict(users)

    df0 = df.select("id", "name").sort(by=col("id"), desc=False).offset(2).limit(0)
    pdf = df0.to_pandas()
    assert len(pdf) == 0, f"Expected 0 rows, got {len(pdf)}"

    df1 = df.select("id", "name").sort(by=col("id"), desc=False).offset(0).limit(7)
    pdf = df1.to_pandas()
    assert len(pdf) == 7, f"Expected 7 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 7)], name="name"), check_names=True, check_index=False
    )

    # 1023..0 -> 1021..0 -> 1021..1014
    df2 = df.select("id", "name").sort(by=col("id"), desc=True).offset(2).limit(7)
    pdf = df2.to_pandas()
    assert len(pdf) == 7, f"Expected 7 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"],
        pd.Series([f"user_{i}" for i in range(1021, 1014, -1)], name="name"),
        check_names=True,
        check_index=False,
    )

    df3 = df.select("id", "name").sort(by=col("id"), desc=False).offset(7).limit(7)
    pdf = df3.to_pandas()
    assert len(pdf) == 7, f"Expected 7 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(7, 14)], name="name"), check_names=True, check_index=False
    )

    # 1016..0 -> 1016..999 -> 1011..1000 -> 1004..1000
    df4 = df.select("id", "name").sort(by=col("id"), desc=True).offset(7).limit(17).offset(5).limit(11).offset(7)
    pdf = df4.to_pandas()
    assert len(pdf) == 4, f"Expected 5 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"],
        pd.Series([f"user_{i}" for i in range(1004, 1000, -1)], name="name"),
        check_names=True,
        check_index=False,
    )

    # 1023..0 -> 999..993 -> 996..993
    df5 = (
        df.select("id", "name")
        .sort(by=col("id"), desc=True)
        .offset(7)
        .offset(17)
        .limit(15)
        .limit(12)
        .limit(6)
        .offset(3)
    )
    pdf = df5.to_pandas()
    assert len(pdf) == 3, f"Expected 3 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"],
        pd.Series([f"user_{i}" for i in range(996, 993, -1)], name="name"),
        check_names=True,
        check_index=False,
    )

    # 516..522 -> 519..522
    df6 = df.select("id", "name").sort(by=col("id"), desc=False)
    for i in range(1, 517):
        df6 = df6.offset(1)
    df6 = df6.limit(15).limit(12).limit(6).offset(3)
    pdf = df6.to_pandas()
    assert len(pdf) == 3, f"Expected 3 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(519, 522)], name="name"), check_names=True, check_index=False
    )
