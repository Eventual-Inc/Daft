from __future__ import annotations

import pytest

import daft
from daft import col
from tests.conftest import get_tests_daft_runner_name


@pytest.fixture(scope="session")
def tmp_data(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("test_limit_offset")
    items = [{"id": i, "name": f"user_{i}", "email": f"user_{i}@getdaft.io"} for i in range(1024)]

    print(f"Generating parquet data for Limit & Offset test, len: {len(items)}, path: {tmp_path}")
    daft.from_pylist(items).write_parquet(root_dir=str(tmp_path))
    return str(tmp_path)


def test_negative_limit(tmp_data):
    df = daft.read_parquet(path=tmp_data)

    with pytest.raises(ValueError) as excinfo:
        df.select("name").limit(-1)
    assert "LIMIT <n> must be greater than or equal to 0, instead got: -1" in str(excinfo.value)


def test_limit(tmp_data):
    df = daft.read_parquet(path=tmp_data)

    df0 = df.select("name").limit(0)
    assert 0 == df0.count_rows()

    df1 = df.select("name").limit(1)
    assert 1 == df1.count_rows()

    df2 = df.select("name").limit(1024)
    assert 1024 == df2.count_rows()

    df3 = df.select("name").limit(9223372036854775807)
    assert 1024 == df3.count_rows()


def test_limit_with_sort(tmp_data):
    df = daft.read_parquet(path=tmp_data)

    df0 = df.sort(by="id").select("name").limit(1)
    assert df0.to_pydict() == {"name": [f"user_{i}" for i in range(0, 1)]}

    df1 = df.select("id", "name").sort(by="id", desc=True).limit(1024)
    assert df1.to_pydict() == {
        "id": [i for i in range(1023, -1, -1)],
        "name": [f"user_{i}" for i in range(1023, -1, -1)],
    }

    df2 = df.sort(by="id").select("name").limit(9223372036854775807)
    assert df2.to_pydict() == {"name": [f"user_{i}" for i in range(0, 1024)]}

    df3 = (
        df.select("id", "name")
        .limit(1024)
        .sort(by=col("id"), desc=False)
        .limit(17)
        .select("name")
        .limit(3)
        .select("name")
    )
    assert df3.to_pydict() == {"name": [f"user_{i}" for i in range(0, 3)]}


@pytest.mark.skipif(
    condition=get_tests_daft_runner_name() != "native",
    reason="Offset operator only implemented in the native runner now",
)
def test_negative_offset(tmp_data):
    df = daft.read_parquet(path=tmp_data)

    with pytest.raises(ValueError) as excinfo:
        df.select("name").offset(-1)
    assert "OFFSET <n> must be greater than or equal to 0, instead got: -1" in str(excinfo.value)


@pytest.mark.skipif(
    condition=get_tests_daft_runner_name() != "native",
    reason="Offset operator only implemented in the native runner now",
)
def test_offset(tmp_data):
    df = daft.read_parquet(path=tmp_data)

    df0 = df.select("name").offset(0)
    assert 1024 == df0.count_rows()

    df1 = df.select("name").offset(1024)
    assert 0 == df1.count_rows()

    df2 = df.select("name").offset(1023)
    assert 1 == df2.count_rows()

    df3 = df.select("name").offset(24)
    assert 1000 == df3.count_rows()

    df4 = df.select("name").offset(1025)
    assert 0 == df4.count_rows()

    df5 = df.select("name").offset(9223372036854775807)
    assert 0 == df5.count_rows()


@pytest.mark.skipif(
    condition=get_tests_daft_runner_name() != "native",
    reason="Offset operator only implemented in the native runner now",
)
def test_limit_offset(tmp_data):
    df = daft.read_parquet(path=tmp_data)

    df0 = df.select("name").limit(7).offset(0)
    assert 7 == df0.count_rows()

    df1 = df.select("name").limit(0).offset(7)
    assert 0 == df1.count_rows()

    df2 = df.select("name").limit(7).offset(2)
    assert 5 == df2.count_rows()

    df3 = df.select("name").limit(7).offset(7)
    assert 0 == df3.count_rows()

    # 0..7 -> 1..7 -> 1..6 -> 3..6
    df4 = df.limit(7).offset(1).select("name").limit(5).offset(2).limit(1024)
    assert 3 == df4.count_rows()

    # 0..17 -> 13..17 -> 13..16
    df5 = df.select("id", "name").limit(1024).limit(17).offset(5).select("name").offset(2).offset(6).limit(3)
    assert 3 == df5.count_rows()

    # 516..1024 -> 516..519
    df6 = df.select("id", "name").limit(1024)
    for i in range(1, 517):
        df6 = df6.offset(1).select("name")
    df6 = df6.limit(3)
    assert 3 == df6.count_rows()


@pytest.mark.skipif(
    condition=get_tests_daft_runner_name() != "native",
    reason="Offset operator only implemented in the native runner now",
)
def test_offset_limit(tmp_data):
    df = daft.read_parquet(path=tmp_data)

    df0 = df.select("name").offset(2).limit(0)
    assert 0 == df0.count_rows()

    df1 = df.select("name").offset(0).limit(7)
    assert 7 == df1.count_rows()

    df2 = df.select("name").offset(2).limit(7)
    assert 7 == df2.count_rows()

    df3 = df.select("name").offset(7).limit(7)
    assert 7 == df3.count_rows()

    # 7..24 -> 12..23 -> 19..23
    df4 = df.select("id", "name").offset(7).limit(17).select("name").offset(5).limit(11).offset(7)
    assert 4 == df4.count_rows()

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
    assert 3 == df5.count_rows()

    # 516..522 -> 519..522
    df6 = df.select("id", "name")
    for i in range(1, 517):
        df6 = df6.offset(1).select("name")
    df6 = df6.limit(15).limit(12).limit(6).select("name").offset(3).select("name")
    assert 3 == df6.count_rows()


@pytest.mark.skipif(
    condition=get_tests_daft_runner_name() != "native",
    reason="Offset operator only implemented in the native runner now",
)
def test_limit_offset_with_sort(tmp_data):
    df = daft.read_parquet(path=tmp_data)

    df0 = df.select("id", "name").sort(by=col("id"), desc=False).limit(7).offset(0)
    assert df0.to_pydict() == {"id": [i for i in range(0, 7)], "name": [f"user_{i}" for i in range(0, 7)]}

    df1 = df.select("id", "name").sort(by=col("id"), desc=True).limit(0).offset(7)
    assert 0 == df1.count_rows()

    # 1023...1016 -> 1021...1016
    df2 = df.select("id", "name").sort(by=col("id"), desc=True).limit(7).offset(2)
    assert df2.to_pydict() == {
        "id": [i for i in range(1021, 1016, -1)],
        "name": [f"user_{i}" for i in range(1021, 1016, -1)],
    }

    df3 = df.select("id", "name").sort(by=col("id"), desc=False).limit(7).offset(7)
    assert 0 == df3.count_rows()

    # 0..7 -> 1..7 -> 1..6 -> 3..6
    df4 = df.select("id", "name").sort(by=col("id"), desc=False).limit(7).offset(1).limit(5).offset(2).limit(1024)
    assert df4.to_pydict() == {"id": [i for i in range(3, 6)], "name": [f"user_{i}" for i in range(3, 6)]}

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
    assert df5.to_pydict() == {"name": [f"user_{i}" for i in range(13, 16)]}

    # 1023..0 -> 507..0 -> 507..504
    df6 = df.select("id", "name").sort(by="id", desc=True).limit(1024)
    for i in range(1, 517):
        df6 = df6.offset(1).select("name")
    df6 = df6.limit(3)
    assert df6.to_pydict() == {"name": [f"user_{i}" for i in range(507, 504, -1)]}


@pytest.mark.skipif(
    condition=get_tests_daft_runner_name() != "native",
    reason="Offset operator only implemented in the native runner now",
)
def test_offset_limit_with_sort(tmp_data):
    df = daft.read_parquet(path=tmp_data)

    df0 = df.select("id", "name").sort(by=col("id"), desc=False).offset(2).limit(0)
    assert 0 == df0.count_rows()

    df1 = df.select("id", "name").sort(by=col("id"), desc=False).offset(0).limit(7)
    assert df1.to_pydict() == {"id": [i for i in range(0, 7)], "name": [f"user_{i}" for i in range(0, 7)]}

    # 1023..0 -> 1021..0 -> 1021..1014
    df2 = df.select("id", "name").sort(by=col("id"), desc=True).offset(2).limit(7)
    assert df2.to_pydict() == {
        "id": [i for i in range(1021, 1014, -1)],
        "name": [f"user_{i}" for i in range(1021, 1014, -1)],
    }

    df3 = df.select("id", "name").sort(by=col("id"), desc=False).offset(7).limit(7)
    assert df3.to_pydict() == {"id": [i for i in range(7, 14)], "name": [f"user_{i}" for i in range(7, 14)]}

    # 1016..0 -> 1016..999 -> 1011..1000 -> 1004..1000
    df4 = df.select("id", "name").sort(by=col("id"), desc=True).offset(7).limit(17).offset(5).limit(11).offset(7)
    assert df4.to_pydict() == {
        "id": [i for i in range(1004, 1000, -1)],
        "name": [f"user_{i}" for i in range(1004, 1000, -1)],
    }

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
    assert df5.to_pydict() == {"name": [f"user_{i}" for i in range(996, 993, -1)]}

    # 516..522 -> 519..522
    df6 = df.select("id", "name").sort(by=col("id"), desc=False)
    for i in range(1, 517):
        df6 = df6.offset(1).select("name")
    df6 = df6.limit(15).limit(12).limit(6).offset(3)
    assert df6.to_pydict() == {"name": [f"user_{i}" for i in range(519, 522)]}
