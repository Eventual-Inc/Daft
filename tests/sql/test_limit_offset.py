from __future__ import annotations

import random

import pandas as pd
import pytest

import daft


def test_limit():
    user = daft.from_pydict({"id": [i for i in range(1024)], "name": [f"user_{i}" for i in range(1024)]})

    df0 = daft.sql("SELECT name FROM user LIMIT 1", **{"user": user})
    pdf = df0.to_pandas()
    assert len(pdf) == 1, f"Expected 1 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 1)], name="name"), check_names=True, check_index=False
    )

    df1 = daft.sql("SELECT name FROM user LIMIT 1024", **{"user": user})
    pdf = df1.to_pandas()
    assert len(pdf) == 1024, f"Expected 1024 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 1024)], name="name"), check_names=True, check_index=False
    )

    df2 = daft.sql("SELECT name FROM user LIMIT 9223372036854775807", **{"user": user})
    pdf = df2.to_pandas()
    assert len(pdf) == 1024, f"Expected 1024 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 1024)], name="name"), check_names=True, check_index=False
    )

    with pytest.raises(Exception) as excinfo:
        daft.sql("SELECT name FROM user LIMIT -1", **{"user": user})
    assert "LIMIT <n> must be greater than or equal to 0, instead got: -1" in str(excinfo.value)

    with pytest.raises(Exception) as excinfo:
        daft.sql("SELECT name FROM user LIMIT 10 LIMIT 7", **{"user": user})
    assert "failed to parse sql: sql parser error: Expected: end of statement, found: LIMIT" in str(excinfo.value)


def test_offset():
    user = daft.from_pydict({"id": [i for i in range(1024)], "name": [f"user_{i}" for i in range(1024)]})

    df0 = daft.sql("SELECT name FROM user OFFSET 0", **{"user": user})
    pdf = df0.to_pandas()
    assert len(pdf) == 1024, f"Expected 1024 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 1024)], name="name"), check_names=True, check_index=False
    )

    df1 = daft.sql("SELECT name FROM user OFFSET 1024", **{"user": user})
    pdf = df1.to_pandas()
    assert len(pdf) == 0, f"Expected 0 rows, got {len(pdf)}"

    df2 = daft.sql("SELECT name FROM user OFFSET 1023", **{"user": user})
    pdf = df2.to_pandas()
    assert len(pdf) == 1, f"Expected 1 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"],
        pd.Series([f"user_{i}" for i in range(1023, 1024)], name="name"),
        check_names=True,
        check_index=False,
    )

    df3 = daft.sql("SELECT name FROM user OFFSET 24", **{"user": user})
    pdf = df3.to_pandas()
    assert len(pdf) == 1000, f"Expected 1000 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(24, 1024)], name="name"), check_names=True, check_index=False
    )

    df4 = daft.sql("SELECT name FROM user OFFSET 1025", **{"user": user})
    pdf = df4.to_pandas()
    assert len(pdf) == 0, f"Expected 0 rows, got {len(pdf)}"

    df5 = daft.sql("SELECT name FROM user OFFSET 9223372036854775807", **{"user": user})
    pdf = df5.to_pandas()
    assert len(pdf) == 0, f"Expected 0 rows, got {len(pdf)}"

    with pytest.raises(Exception) as excinfo:
        daft.sql("SELECT name FROM user OFFSET -1", **{"user": user})
    assert "OFFSET <n> must be greater than or equal to 0, instead got: -1" in str(excinfo.value)

    with pytest.raises(Exception) as excinfo:
        daft.sql("SELECT name FROM user OFFSET 7 OFFSET 10", **{"user": user})
    assert "failed to parse sql: sql parser error: Expected: end of statement, found: OFFSET" in str(excinfo.value)


def test_limit_offset():
    user = daft.from_pydict({"id": [i for i in range(1024)], "name": [f"user_{i}" for i in range(1024)]})

    df0 = daft.sql("SELECT name FROM user LIMIT 7 OFFSET 0", **{"user": user})
    pdf = df0.to_pandas()
    assert len(pdf) == 7, f"Expected 7 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 7)], name="name"), check_names=True, check_index=False
    )
    pd.testing.assert_frame_equal(
        daft.sql("SELECT name FROM user OFFSET 0 LIMIT 7", **{"user": user}).to_pandas(), pdf, check_names=True
    )

    df1 = daft.sql("SELECT name FROM user LIMIT 0 OFFSET 7", **{"user": user})
    pdf = df1.to_pandas()
    assert len(pdf) == 0, f"Expected 0 rows, got {len(pdf)}"
    pd.testing.assert_frame_equal(
        daft.sql("SELECT name FROM user OFFSET 7 LIMIT 0", **{"user": user}).to_pandas(), pdf, check_names=True
    )

    df2 = daft.sql("SELECT name FROM user LIMIT 7 OFFSET 2", **{"user": user})
    pdf = df2.to_pandas()
    assert len(pdf) == 5, f"Expected 5 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(2, 7)], name="name"), check_names=True, check_index=False
    )
    pd.testing.assert_frame_equal(
        daft.sql("SELECT name FROM user OFFSET 2 LIMIT 7", **{"user": user}).to_pandas(), pdf, check_names=True
    )

    df3 = daft.sql("SELECT name FROM user LIMIT 7 OFFSET 7", **{"user": user})
    pdf = df3.to_pandas()
    assert len(pdf) == 0, f"Expected 0 rows, got {len(pdf)}"
    pd.testing.assert_frame_equal(
        daft.sql("SELECT name FROM user OFFSET 7 LIMIT 7", **{"user": user}).to_pandas(), pdf, check_names=True
    )

    with pytest.raises(Exception) as excinfo:
        daft.sql("SELECT name FROM user LIMIT 7 OFFSET 1 LIMIT 5", **{"user": user})
    assert "failed to parse sql: sql parser error: Expected: end of statement, found: LIMIT" in str(excinfo.value)

    with pytest.raises(Exception) as excinfo:
        daft.sql("SELECT name FROM user OFFSET 1 LIMIT 7 OFFSET 5", **{"user": user})
    assert "failed to parse sql: sql parser error: Expected: end of statement, found: OFFSET" in str(excinfo.value)


def test_limit_offset_with_sort():
    uids = [i for i in range(1024)]
    random.shuffle(uids)
    users = {"id": [], "name": []}
    for uid in uids:
        users["id"].append(uid)
        users["name"].append(f"user_{uid}")

    user = daft.from_pydict(users)

    df0 = daft.sql("SELECT id, name FROM user ORDER BY id LIMIT 7 OFFSET 0", **{"user": user})
    pdf = df0.to_pandas()
    assert len(pdf) == 7, f"Expected 7 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"], pd.Series([f"user_{i}" for i in range(0, 7)], name="name"), check_names=True, check_index=False
    )
    pd.testing.assert_frame_equal(
        daft.sql("SELECT id, name FROM user ORDER BY id OFFSET 0 LIMIT 7", **{"user": user}).to_pandas(),
        pdf,
        check_names=True,
    )

    df1 = daft.sql("SELECT id, name FROM user ORDER BY id DESC LIMIT 0 OFFSET 7", **{"user": user})
    pdf = df1.to_pandas()
    assert len(pdf) == 0, f"Expected 0 rows, got {len(pdf)}"
    pd.testing.assert_frame_equal(
        daft.sql("SELECT id, name FROM user ORDER BY id DESC OFFSET 7 LIMIT 0", **{"user": user}).to_pandas(),
        pdf,
        check_names=True,
    )

    # 1023...1016 -> 1021...1016
    df2 = daft.sql("SELECT id, name FROM user ORDER BY id DESC LIMIT 7 OFFSET 2", **{"user": user})
    pdf = df2.to_pandas()
    assert len(pdf) == 5, f"Expected 5 rows, got {len(pdf)}"
    pd.testing.assert_series_equal(
        pdf["name"],
        pd.Series([f"user_{i}" for i in range(1021, 1016, -1)], name="name"),
        check_names=True,
        check_index=False,
    )
    pd.testing.assert_frame_equal(
        daft.sql("SELECT id, name FROM user ORDER BY id DESC OFFSET 2 LIMIT 7", **{"user": user}).to_pandas(),
        pdf,
        check_names=True,
    )

    df3 = daft.sql("SELECT id, name FROM user ORDER BY id LIMIT 7 OFFSET 7", **{"user": user})
    pdf = df3.to_pandas()
    assert len(pdf) == 0, f"Expected 0 rows, got {len(pdf)}"
    pd.testing.assert_frame_equal(
        daft.sql("SELECT id, name FROM user ORDER BY id OFFSET 7 LIMIT 7", **{"user": user}).to_pandas(),
        pdf,
        check_names=True,
    )
