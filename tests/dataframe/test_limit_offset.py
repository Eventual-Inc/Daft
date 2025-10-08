from __future__ import annotations

import pytest

import daft
from daft import DataType, col


@pytest.fixture
def input_df():
    df = daft.range(start=0, end=1024, partitions=100)
    df = df.with_columns(
        {
            "name": df["id"].apply(func=lambda x: f"user_{x}", return_dtype=DataType.string()),
            "email": df["id"].apply(func=lambda x: f"user_{x}@getdaft.io", return_dtype=DataType.string()),
        }
    )
    return df


def test_negative_limit(input_df):
    with pytest.raises(ValueError) as excinfo:
        input_df.select("name").limit(-1)
    assert "LIMIT <n> must be greater than or equal to 0, instead got: -1" in str(excinfo.value)


def test_limit(input_df):
    df = input_df.select("name").limit(0)
    assert df.count_rows() == 0

    df = input_df.select("id", "name").filter(col("id") > 127).limit(1)
    assert df.count_rows() == 1

    df = input_df.select("name").limit(1024)
    assert df.count_rows() == 1024

    df = input_df.select("id", "name").filter(col("id") >= 25).limit(1024)
    assert df.count_rows() == 999

    df = input_df.select("id", "name").filter(col("id") >= 1023).limit(1024)
    assert df.count_rows() == 1

    df = input_df.select("id", "name").filter(col("id") > 1023).limit(1024)
    assert df.count_rows() == 0

    df = input_df.select("name").limit(9223372036854775807)
    assert df.count_rows() == 1024


def test_limit_with_sort(input_df):
    df = input_df.sort(by="id").select("name").limit(1)
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(0, 1)]}

    df = input_df.sort(by="id").filter(col("id") > 17).select("name").limit(1)
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(18, 19)]}

    df = input_df.sort(by="id").filter(col("id") > 17).limit(185).filter(col("id") < 100).select("name")
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(18, 100)]}

    df = input_df.sort(by="id").filter(col("id") > 17).limit(15).filter(col("id") < 100).select("name")
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(18, 33)]}

    df = input_df.select("id", "name").sort(by="id", desc=True).limit(1024)
    assert df.to_pydict() == {
        "id": [i for i in range(1023, -1, -1)],
        "name": [f"user_{i}" for i in range(1023, -1, -1)],
    }

    df = input_df.select("id", "name").sort(by="id", desc=True).filter(col("id") >= 24).limit(1024)
    assert df.to_pydict() == {
        "id": [i for i in range(1023, 23, -1)],
        "name": [f"user_{i}" for i in range(1023, 23, -1)],
    }

    df = input_df.select("id", "name").sort(by="id", desc=True).filter(col("id") >= 24).limit(1000)
    assert df.to_pydict() == {
        "id": [i for i in range(1023, 23, -1)],
        "name": [f"user_{i}" for i in range(1023, 23, -1)],
    }

    df = input_df.select("id", "name").sort(by="id", desc=True).filter(col("id") >= 24).limit(100)
    assert df.to_pydict() == {
        "id": [i for i in range(1023, 923, -1)],
        "name": [f"user_{i}" for i in range(1023, 923, -1)],
    }

    df = (
        input_df.select("id", "name")
        .sort(by="id", desc=True)
        .filter(col("id") >= 24)
        .limit(100)
        .filter(col("id") < 1000)
    )
    assert df.to_pydict() == {
        "id": [i for i in range(999, 923, -1)],
        "name": [f"user_{i}" for i in range(999, 923, -1)],
    }

    df = (
        input_df.select("id", "name")
        .sort(by="id", desc=True)
        .filter(col("id") >= 24)
        .limit(100)
        .filter(col("id") < 100)
    )
    assert df.to_pydict() == {
        "id": [],
        "name": [],
    }

    df = input_df.sort(by="id").select("name").limit(9223372036854775807)
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(0, 1024)]}

    df = (
        input_df.select("id", "name")
        .limit(1024)
        .sort(by="id", desc=False)
        .filter(col("id") > 24)
        .limit(17)
        .select("name")
        .limit(3)
        .select("name")
    )
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(25, 28)]}

    df = (
        input_df.select("id", "name")
        .limit(1024)
        .sort(by="id", desc=False)
        .filter(col("id") > 24)
        .limit(17)
        .filter(col("id") < 25)
        .limit(3)
        .select("name")
    )
    assert df.to_pydict() == {"name": []}


def test_negative_offset(input_df):
    with pytest.raises(ValueError) as excinfo:
        input_df.select("name").offset(-1).limit(1).collect()
    assert "OFFSET <n> must be greater than or equal to 0, instead got: -1" in str(excinfo.value)


def test_offset_without_limit(input_df):
    with pytest.raises(Exception) as excinfo:
        input_df.select("name").offset(17).collect()
    assert "Not Yet Implemented: Offset without limit is unsupported now!" in str(excinfo.value)


def test_limit_before_offset(input_df):
    df = input_df.select("name").limit(7).offset(0)
    assert df.count_rows() == 7

    df = input_df.select("name").limit(0).offset(7)
    assert df.count_rows() == 0

    df = input_df.select("id", "name").filter(col("id") > 511).limit(7).offset(2)
    assert df.count_rows() == 5

    df = input_df.select("id", "name").filter(col("id") > 511).limit(7).offset(2).filter(col("id") < 512)
    assert df.count_rows() == 0

    df = input_df.select("name").limit(7).offset(7)
    assert df.count_rows() == 0

    # 0..7 -> 1..7 -> 1..6 -> 3..6
    df = input_df.limit(7).offset(1).select("name").limit(5).offset(2).limit(1024)
    assert df.count_rows() == 3

    # 0..17 -> 13..17 -> 13..16
    df = input_df.select("id", "name").limit(1024).limit(17).offset(5).select("name").offset(2).offset(6).limit(3)
    assert df.count_rows() == 3

    # 999..1016 -> 1012..1016 -> 1012..1015
    df = (
        input_df.select("id", "name")
        .filter(col("id") >= 999)
        .limit(1024)
        .limit(17)
        .offset(5)
        .select("name")
        .offset(2)
        .offset(6)
        .limit(3)
    )
    assert df.count_rows() == 3

    # 516..1024 -> 516..519
    df = input_df.select("id", "name").limit(1024)
    for i in range(1, 517):
        df = df.offset(1)
    df = df.select("name").limit(3)
    assert df.count_rows() == 3


def test_limit_after_offset(input_df):
    df = input_df.select("name").offset(2).limit(0)
    assert df.count_rows() == 0

    df = input_df.select("name").offset(0).limit(7)
    assert df.count_rows() == 7

    df = input_df.select("name").offset(2).limit(7)
    assert df.count_rows() == 7

    df = input_df.select("id", "name").filter(col("id") > 997).select("name").offset(2).limit(7)
    assert df.count_rows() == 7

    df = input_df.select("id", "name").filter(col("id") > 1020).select("name").offset(2).limit(7)
    assert df.count_rows() == 1

    df = input_df.select("name").offset(7).limit(7)
    assert df.count_rows() == 7

    # 7..24 -> 12..23 -> 19..23
    df = input_df.select("id", "name").offset(7).limit(17).select("name").offset(5).limit(11).offset(7)
    assert df.count_rows() == 4

    # 704..721 -> 709..720 -> 716..720
    df = (
        input_df.select("id", "name")
        .filter(col("id") > 697)
        .offset(7)
        .limit(17)
        .select("name")
        .offset(5)
        .limit(11)
        .offset(7)
    )
    assert df.count_rows() == 4

    # 24..30 -> 27..30
    df = (
        input_df.offset(7)
        .select("id", "name")
        .offset(17)
        .limit(15)
        .limit(12)
        .select("name")
        .limit(6)
        .offset(3)
        .select("name")
    )
    assert df.count_rows() == 3

    # 516..522 -> 519..522
    df = input_df.select("id", "name")
    for i in range(1, 517):
        df = df.offset(1)
    df = df.limit(15).limit(12).limit(6).select("name").offset(3).select("name")
    assert df.count_rows() == 3


def test_limit_before_offset_with_sort(input_df):
    df = input_df.select("id", "name").sort(by=col("id"), desc=False).limit(7).offset(0)
    assert df.to_pydict() == {"id": [i for i in range(0, 7)], "name": [f"user_{i}" for i in range(0, 7)]}

    df = input_df.select("id", "name").sort(by=col("id"), desc=True).limit(0).offset(7)
    assert df.count_rows() == 0

    # 1023...1016 -> 1021...1016
    df = input_df.select("id", "name").sort(by=col("id"), desc=True).limit(7).offset(2)
    assert df.to_pydict() == {
        "id": [i for i in range(1021, 1016, -1)],
        "name": [f"user_{i}" for i in range(1021, 1016, -1)],
    }

    # 126...119 -> 124...119
    df = input_df.select("id", "name").sort(by=col("id"), desc=True).filter(col("id") < 127).limit(7).offset(2)
    assert df.to_pydict() == {
        "id": [i for i in range(124, 119, -1)],
        "name": [f"user_{i}" for i in range(124, 119, -1)],
    }

    # 126...119 -> 124...121
    df = (
        input_df.select("id", "name")
        .sort(by=col("id"), desc=True)
        .filter(col("id") < 127)
        .limit(7)
        .offset(2)
        .filter(col("id") >= 122)
    )
    assert df.to_pydict() == {
        "id": [i for i in range(124, 121, -1)],
        "name": [f"user_{i}" for i in range(124, 121, -1)],
    }

    df = input_df.select("id", "name").sort(by=col("id"), desc=False).limit(7).offset(7)
    assert df.count_rows() == 0

    # 0..7 -> 1..7 -> 1..6 -> 3..6
    df = input_df.select("id", "name").sort(by=col("id"), desc=False).limit(7).offset(1).limit(5).offset(2).limit(1024)
    assert df.to_pydict() == {"id": [i for i in range(3, 6)], "name": [f"user_{i}" for i in range(3, 6)]}

    # 700..707 -> 701..707 -> 701..706 -> 703..706
    df = (
        input_df.select("id", "name")
        .sort(by=col("id"), desc=False)
        .filter(col("id") >= 700)
        .limit(7)
        .offset(1)
        .limit(5)
        .offset(2)
        .limit(1024)
    )
    assert df.to_pydict() == {"id": [i for i in range(703, 706)], "name": [f"user_{i}" for i in range(703, 706)]}

    # 0..17 -> 13..17 -> 13..16
    df = (
        input_df.select("id", "name")
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
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(13, 16)]}

    # 13..30 -> 26..30 -> 26..29
    df = (
        input_df.select("id", "name")
        .limit(1024)
        .sort(by=col("id"), desc=False)
        .filter(col("id") >= 13)
        .limit(17)
        .offset(5)
        .select("name")
        .offset(2)
        .offset(6)
        .limit(3)
        .select("name")
    )
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(26, 29)]}

    # 1023..0 -> 507..0 -> 507..504
    df = input_df.select("id", "name").sort(by="id", desc=True).limit(1024)
    for i in range(1, 517):
        df = df.offset(1)
    df = df.limit(3).select("name")
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(507, 504, -1)]}

    # 518..0 -> 2..0
    df = input_df.select("id", "name").sort(by="id", desc=True).filter(col("id") < 519).limit(1024)
    for i in range(1, 517):
        df = df.offset(1)
    df = df.limit(3).select("name")
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(2, -1, -1)]}


def test_limit_after_offset_with_sort(input_df):
    df = input_df.select("id", "name").sort(by=col("id"), desc=False).offset(2).limit(0)
    assert df.count_rows() == 0

    df = input_df.select("id", "name").sort(by=col("id"), desc=False).offset(0).limit(7)
    assert df.to_pydict() == {"id": [i for i in range(0, 7)], "name": [f"user_{i}" for i in range(0, 7)]}

    # 1023..0 -> 1021..0 -> 1021..1014
    df = input_df.select("id", "name").sort(by=col("id"), desc=True).offset(2).limit(7)
    assert df.to_pydict() == {
        "id": [i for i in range(1021, 1014, -1)],
        "name": [f"user_{i}" for i in range(1021, 1014, -1)],
    }

    # 100..0 -> 98..91
    df = input_df.select("id", "name").sort(by=col("id"), desc=True).filter(col("id") <= 100).offset(2).limit(7)
    assert df.to_pydict() == {
        "id": [i for i in range(98, 91, -1)],
        "name": [f"user_{i}" for i in range(98, 91, -1)],
    }

    df = input_df.select("id", "name").sort(by=col("id"), desc=False).offset(7).limit(7)
    assert df.to_pydict() == {"id": [i for i in range(7, 14)], "name": [f"user_{i}" for i in range(7, 14)]}

    # 1016..0 -> 1016..999 -> 1011..1000 -> 1004..1000
    df = input_df.select("id", "name").sort(by=col("id"), desc=True).offset(7).limit(17).offset(5).limit(11).offset(7)
    assert df.to_pydict() == {
        "id": [i for i in range(1004, 1000, -1)],
        "name": [f"user_{i}" for i in range(1004, 1000, -1)],
    }

    # 1016..1005 -> 1016..1005 -> 1011..1005 -> 1004..1005
    df = (
        input_df.select("id", "name")
        .sort(by=col("id"), desc=True)
        .filter(col("id") > 1004)
        .offset(7)
        .limit(17)
        .offset(5)
        .limit(11)
        .offset(7)
    )
    assert df.to_pydict() == {
        "id": [],
        "name": [],
    }

    # 1023..0 -> 999..993 -> 996..993
    df = (
        input_df.select("id", "name")
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
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(996, 993, -1)]}

    # 523..0 -> 499..493 -> 496..493
    df = (
        input_df.select("id", "name")
        .limit(1024)
        .sort(by=col("id"), desc=True)
        .filter(col("id") <= 523)
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
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(496, 493, -1)]}

    # 516..522 -> 519..522
    df = input_df.select("id", "name").sort(by=col("id"), desc=False)
    for i in range(1, 517):
        df = df.offset(1).select("name")
    df = df.limit(15).limit(12).limit(6).offset(3)
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(519, 522)]}

    # 516..518 -> 519..518
    df = input_df.select("id", "name").sort(by=col("id"), desc=False).filter(col("id") < 519)
    for i in range(1, 517):
        df = df.offset(1).select("name")
    df = df.limit(15).limit(12).limit(6).offset(3)
    assert df.to_pydict() == {"name": []}


def test_paging(input_df):
    offset = 0
    limit = 100

    total = input_df.count_rows()
    while offset < total:
        paged_df = input_df.select("id", "name").sort(by=col("id"), desc=False).offset(offset).limit(limit)

        assert paged_df.to_pydict() == {
            "id": [i for i in range(offset, min(total, offset + limit))],
            "name": [f"user_{i}" for i in range(offset, min(total, offset + limit))],
        }

        offset += limit


def test_multiple_limits():
    df = daft.range(1000, partitions=100)
    df = df.filter(daft.col("id") > 100).limit(900)
    df = df.filter(daft.col("id") > 200).limit(800)
    df = df.filter(daft.col("id") > 300).limit(700)
    df = df.filter(daft.col("id") > 400).limit(600)
    df = df.filter(daft.col("id") > 500).limit(500)
    df = df.filter(daft.col("id") > 600).limit(400)
    df = df.filter(daft.col("id") > 700).limit(300)
    df = df.filter(daft.col("id") > 800).limit(200)
    df = df.filter(daft.col("id") > 900).limit(100)

    df = df.to_pydict()
    assert df["id"] == [i for i in range(901, 1000)]
