from __future__ import annotations

import pytest

import daft
from daft import col
from daft.functions import format


@pytest.fixture(params=["memory"], scope="session")
def input_df(request, tmp_path_factory):
    with daft.execution_config_ctx(enable_dynamic_batching=True):
        df = daft.range(start=0, end=1024, partitions=100)
        df = df.with_columns(
            {
                "name": format("user_{}", df["id"]),
                "email": format("user_{}@daft.ai", df["id"]),
            }
        )

        path = str(tmp_path_factory.mktemp(request.param))
        if request.param == "parquet":
            df.write_parquet(path)
            return daft.read_parquet(path)
        elif request.param == "lance":
            lance = pytest.importorskip("lance")
            lance.write_dataset(df.to_arrow(), path)
            return daft.read_lance(path)
        else:
            return df


def test_negative_limit(input_df):
    with pytest.raises(ValueError) as excinfo:
        input_df.select("name").limit(-1)
    assert "LIMIT <n> must be greater than or equal to 0, instead got: -1" in str(
        excinfo.value
    )


@pytest.mark.parametrize(
    "df_fn,expected_count",
    [
        pytest.param(
            lambda input_df: input_df.select("name").limit(0), 0, id="limit_0"
        ),
        pytest.param(
            lambda input_df: input_df.select("id", "name")
            .filter(col("id") > 127)
            .limit(1),
            1,
            id="filter_gt_127_then_limit_1",
        ),
        pytest.param(
            lambda input_df: input_df.select("name").limit(1024),
            1024,
            id="limit_equal_total",
        ),
        pytest.param(
            lambda input_df: input_df.select("id", "name")
            .filter(col("id") >= 25)
            .limit(1024),
            999,
            id="filter_ge_25_limit_1024",
        ),
        pytest.param(
            lambda input_df: input_df.select("id", "name")
            .filter(col("id") >= 1023)
            .limit(1024),
            1,
            id="filter_ge_1023_limit_1024",
        ),
        pytest.param(
            lambda input_df: input_df.select("id", "name")
            .filter(col("id") > 1023)
            .limit(1024),
            0,
            id="filter_gt_1023_limit_1024",
        ),
        pytest.param(
            lambda input_df: input_df.select("name").limit(9223372036854775807),
            1024,
            id="large_limit",
        ),
    ],
)
def test_limit(input_df, df_fn, expected_count):
    df = df_fn(input_df)
    assert df.count_rows() == expected_count


@pytest.mark.parametrize(
    "df_fn,expected_dict",
    [
        pytest.param(
            lambda input_df: input_df.sort(by="id").select("name").limit(1),
            {"name": [f"user_{i}" for i in range(0, 1)]},
            id="sort_asc_limit_1",
        ),
        pytest.param(
            lambda input_df: input_df.sort(by="id")
            .filter(col("id") > 17)
            .select("name")
            .limit(1),
            {"name": [f"user_{i}" for i in range(18, 19)]},
            id="sort_asc_filter_gt_17_limit_1",
        ),
        pytest.param(
            lambda input_df: (
                input_df.sort(by="id")
                .filter(col("id") > 17)
                .limit(185)
                .filter(col("id") < 100)
                .select("name")
            ),
            {"name": [f"user_{i}" for i in range(18, 100)]},
            id="sort_asc_filter_gt_17_limit_185_filter_lt_100",
        ),
        pytest.param(
            lambda input_df: (
                input_df.sort(by="id")
                .filter(col("id") > 17)
                .limit(15)
                .filter(col("id") < 100)
                .select("name")
            ),
            {"name": [f"user_{i}" for i in range(18, 33)]},
            id="sort_asc_filter_gt_17_limit_15_filter_lt_100",
        ),
        pytest.param(
            lambda input_df: input_df.select("id", "name")
            .sort(by="id", desc=True)
            .limit(1024),
            {
                "id": [i for i in range(1023, -1, -1)],
                "name": [f"user_{i}" for i in range(1023, -1, -1)],
            },
            id="sort_desc_limit_1024",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .sort(by="id", desc=True)
                .filter(col("id") >= 24)
                .limit(1024)
            ),
            {
                "id": [i for i in range(1023, 23, -1)],
                "name": [f"user_{i}" for i in range(1023, 23, -1)],
            },
            id="sort_desc_filter_ge_24_limit_1024",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .sort(by="id", desc=True)
                .filter(col("id") >= 24)
                .limit(1000)
            ),
            {
                "id": [i for i in range(1023, 23, -1)],
                "name": [f"user_{i}" for i in range(1023, 23, -1)],
            },
            id="sort_desc_filter_ge_24_limit_1000",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .sort(by="id", desc=True)
                .filter(col("id") >= 24)
                .limit(100)
            ),
            {
                "id": [i for i in range(1023, 923, -1)],
                "name": [f"user_{i}" for i in range(1023, 923, -1)],
            },
            id="sort_desc_filter_ge_24_limit_100",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .sort(by="id", desc=True)
                .filter(col("id") >= 24)
                .limit(100)
                .filter(col("id") < 1000)
            ),
            {
                "id": [i for i in range(999, 923, -1)],
                "name": [f"user_{i}" for i in range(999, 923, -1)],
            },
            id="sort_desc_filter_ge_24_limit_100_filter_lt_1000",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .sort(by="id", desc=True)
                .filter(col("id") >= 24)
                .limit(100)
                .filter(col("id") < 100)
            ),
            {"id": [], "name": []},
            id="sort_desc_filter_ge_24_limit_100_filter_lt_100",
        ),
        pytest.param(
            lambda input_df: input_df.sort(by="id")
            .select("name")
            .limit(9223372036854775807),
            {"name": [f"user_{i}" for i in range(0, 1024)]},
            id="sort_asc_large_limit",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .limit(1024)
                .sort(by="id", desc=False)
                .filter(col("id") > 24)
                .limit(17)
                .select("name")
                .limit(3)
                .select("name")
            ),
            {"name": [f"user_{i}" for i in range(25, 28)]},
            id="limit_sort_filter_multiple_limits",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .limit(1024)
                .sort(by="id", desc=False)
                .filter(col("id") > 24)
                .limit(17)
                .filter(col("id") < 25)
                .limit(3)
                .select("name")
            ),
            {"name": []},
            id="limit_sort_filter_conflicting_filters",
        ),
    ],
)
def test_limit_with_sort(input_df, df_fn, expected_dict):
    df = df_fn(input_df)
    assert df.to_pydict() == expected_dict


def test_negative_offset(input_df):
    with pytest.raises(ValueError) as excinfo:
        input_df.select("name").offset(-1).limit(1).collect()
    assert "OFFSET <n> must be greater than or equal to 0, instead got: -1" in str(
        excinfo.value
    )


def test_offset_without_limit(input_df):
    with pytest.raises(Exception) as excinfo:
        input_df.select("name").offset(17).collect()
    assert "Not Yet Implemented: Offset without limit is unsupported now!" in str(
        excinfo.value
    )


@pytest.mark.parametrize(
    "df_fn,expected_count",
    [
        pytest.param(
            lambda input_df: input_df.select("name").limit(7).offset(0),
            7,
            id="limit_7_offset_0",
        ),
        pytest.param(
            lambda input_df: input_df.select("name").limit(0).offset(7),
            0,
            id="limit_0_offset_7",
        ),
        pytest.param(
            lambda input_df: input_df.select("id", "name")
            .filter(col("id") > 511)
            .limit(7)
            .offset(2),
            5,
            id="filter_gt_511_limit_7_offset_2",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .filter(col("id") > 511)
                .limit(7)
                .offset(2)
                .filter(col("id") < 512)
            ),
            0,
            id="filter_gt_511_limit_7_offset_2_filter_lt_512",
        ),
        pytest.param(
            lambda input_df: input_df.select("name").limit(7).offset(7),
            0,
            id="limit_7_offset_7",
        ),
        pytest.param(
            lambda input_df: input_df.limit(7)
            .offset(1)
            .select("name")
            .limit(5)
            .offset(2)
            .limit(1024),
            3,
            id="limit_7_offset_1_limit_5_offset_2",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .limit(1024)
                .limit(17)
                .offset(5)
                .select("name")
                .offset(2)
                .offset(6)
                .limit(3)
            ),
            3,
            id="limit_1024_limit_17_offset_5_offset_2_offset_6_limit_3",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .filter(col("id") >= 999)
                .limit(1024)
                .limit(17)
                .offset(5)
                .select("name")
                .offset(2)
                .offset(6)
                .limit(3)
            ),
            3,
            id="filter_ge_999_limit_1024_limit_17_offset_5_offset_2_offset_6_limit_3",
        ),
    ],
)
def test_limit_before_offset(input_df, df_fn, expected_count):
    df = df_fn(input_df)
    assert df.count_rows() == expected_count


def test_limit_before_offset_loop(input_df):
    # 516..1024 -> 516..519
    df = input_df.select("id", "name").limit(1024)
    for i in range(1, 517):
        df = df.offset(1)
    df = df.select("name").limit(3)
    assert df.count_rows() == 3


@pytest.mark.parametrize(
    "df_fn,expected_count",
    [
        pytest.param(
            lambda input_df: input_df.select("name").offset(2).limit(0),
            0,
            id="offset_2_limit_0",
        ),
        pytest.param(
            lambda input_df: input_df.select("name").offset(0).limit(7),
            7,
            id="offset_0_limit_7",
        ),
        pytest.param(
            lambda input_df: input_df.select("name").offset(2).limit(7),
            7,
            id="offset_2_limit_7",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .filter(col("id") > 997)
                .select("name")
                .offset(2)
                .limit(7)
            ),
            7,
            id="filter_gt_997_offset_2_limit_7",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .filter(col("id") > 1020)
                .select("name")
                .offset(2)
                .limit(7)
            ),
            1,
            id="filter_gt_1020_offset_2_limit_7",
        ),
        pytest.param(
            lambda input_df: input_df.select("name").offset(7).limit(7),
            7,
            id="offset_7_limit_7",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .offset(7)
                .limit(17)
                .select("name")
                .offset(5)
                .limit(11)
                .offset(7)
            ),
            4,
            id="offset_7_limit_17_offset_5_limit_11_offset_7",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .filter(col("id") > 697)
                .offset(7)
                .limit(17)
                .select("name")
                .offset(5)
                .limit(11)
                .offset(7)
            ),
            4,
            id="filter_gt_697_offset_7_limit_17_offset_5_limit_11_offset_7",
        ),
        pytest.param(
            lambda input_df: (
                input_df.offset(7)
                .select("id", "name")
                .offset(17)
                .limit(15)
                .limit(12)
                .select("name")
                .limit(6)
                .offset(3)
                .select("name")
            ),
            3,
            id="offset_7_offset_17_limit_15_limit_12_limit_6_offset_3",
        ),
    ],
)
def test_limit_after_offset(input_df, df_fn, expected_count):
    df = df_fn(input_df)
    assert df.count_rows() == expected_count


def test_limit_after_offset_loop(input_df):
    # 516..522 -> 519..522
    df = input_df.select("id", "name")
    for i in range(1, 517):
        df = df.offset(1)
    df = df.limit(15).limit(12).limit(6).select("name").offset(3).select("name")
    assert df.count_rows() == 3


@pytest.mark.parametrize(
    "df_fn,expected_result",
    [
        pytest.param(
            lambda input_df: input_df.select("id", "name")
            .sort(by=col("id"), desc=False)
            .limit(7)
            .offset(0),
            {
                "id": [i for i in range(0, 7)],
                "name": [f"user_{i}" for i in range(0, 7)],
            },
            id="sort_asc_limit_7_offset_0",
        ),
        pytest.param(
            lambda input_df: input_df.select("id", "name")
            .sort(by=col("id"), desc=True)
            .limit(0)
            .offset(7),
            0,
            id="sort_desc_limit_0_offset_7",
        ),
        pytest.param(
            lambda input_df: input_df.select("id", "name")
            .sort(by=col("id"), desc=True)
            .limit(7)
            .offset(2),
            {
                "id": [i for i in range(1021, 1016, -1)],
                "name": [f"user_{i}" for i in range(1021, 1016, -1)],
            },
            id="sort_desc_limit_7_offset_2",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .sort(by=col("id"), desc=True)
                .filter(col("id") < 127)
                .limit(7)
                .offset(2)
            ),
            {
                "id": [i for i in range(124, 119, -1)],
                "name": [f"user_{i}" for i in range(124, 119, -1)],
            },
            id="sort_desc_filter_lt_127_limit_7_offset_2",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .sort(by=col("id"), desc=True)
                .filter(col("id") < 127)
                .limit(7)
                .offset(2)
                .filter(col("id") >= 122)
            ),
            {
                "id": [i for i in range(124, 121, -1)],
                "name": [f"user_{i}" for i in range(124, 121, -1)],
            },
            id="sort_desc_filter_lt_127_limit_7_offset_2_filter_ge_122",
        ),
        pytest.param(
            lambda input_df: input_df.select("id", "name")
            .sort(by=col("id"), desc=False)
            .limit(7)
            .offset(7),
            0,
            id="sort_asc_limit_7_offset_7",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .sort(by=col("id"), desc=False)
                .limit(7)
                .offset(1)
                .limit(5)
                .offset(2)
                .limit(1024)
            ),
            {
                "id": [i for i in range(3, 6)],
                "name": [f"user_{i}" for i in range(3, 6)],
            },
            id="sort_asc_limit_7_offset_1_limit_5_offset_2",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .sort(by=col("id"), desc=False)
                .filter(col("id") >= 700)
                .limit(7)
                .offset(1)
                .limit(5)
                .offset(2)
                .limit(1024)
            ),
            {
                "id": [i for i in range(703, 706)],
                "name": [f"user_{i}" for i in range(703, 706)],
            },
            id="sort_asc_filter_ge_700_limit_7_offset_1_limit_5_offset_2",
        ),
        pytest.param(
            lambda input_df: (
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
            ),
            {"name": [f"user_{i}" for i in range(13, 16)]},
            id="limit_1024_sort_asc_limit_17_offset_5_offset_2_offset_6_limit_3",
        ),
        pytest.param(
            lambda input_df: (
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
            ),
            {"name": [f"user_{i}" for i in range(26, 29)]},
            id="limit_1024_sort_asc_filter_ge_13_limit_17_offset_5_offset_2_offset_6_limit_3",
        ),
    ],
)
def test_limit_before_offset_with_sort(input_df, df_fn, expected_result):
    df = df_fn(input_df)
    if isinstance(expected_result, int):
        assert df.count_rows() == expected_result
    else:
        assert df.to_pydict() == expected_result


def test_limit_before_offset_with_sort_loop_1(input_df):
    # 1023..0 -> 507..0 -> 507..504
    df = input_df.select("id", "name").sort(by="id", desc=True).limit(1024)
    for i in range(1, 517):
        df = df.offset(1)
    df = df.limit(3).select("name")
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(507, 504, -1)]}


def test_limit_before_offset_with_sort_loop_2(input_df):
    # 518..0 -> 2..0
    df = (
        input_df.select("id", "name")
        .sort(by="id", desc=True)
        .filter(col("id") < 519)
        .limit(1024)
    )
    for i in range(1, 517):
        df = df.offset(1)
    df = df.limit(3).select("name")
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(2, -1, -1)]}


@pytest.mark.parametrize(
    "df_fn,expected_result",
    [
        pytest.param(
            lambda input_df: input_df.select("id", "name")
            .sort(by=col("id"), desc=False)
            .offset(2)
            .limit(0),
            0,
            id="sort_asc_offset_2_limit_0",
        ),
        pytest.param(
            lambda input_df: input_df.select("id", "name")
            .sort(by=col("id"), desc=False)
            .offset(0)
            .limit(7),
            {
                "id": [i for i in range(0, 7)],
                "name": [f"user_{i}" for i in range(0, 7)],
            },
            id="sort_asc_offset_0_limit_7",
        ),
        pytest.param(
            lambda input_df: input_df.select("id", "name")
            .sort(by=col("id"), desc=True)
            .offset(2)
            .limit(7),
            {
                "id": [i for i in range(1021, 1014, -1)],
                "name": [f"user_{i}" for i in range(1021, 1014, -1)],
            },
            id="sort_desc_offset_2_limit_7",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .sort(by=col("id"), desc=True)
                .filter(col("id") <= 100)
                .offset(2)
                .limit(7)
            ),
            {
                "id": [i for i in range(98, 91, -1)],
                "name": [f"user_{i}" for i in range(98, 91, -1)],
            },
            id="sort_desc_filter_le_100_offset_2_limit_7",
        ),
        pytest.param(
            lambda input_df: input_df.select("id", "name")
            .sort(by=col("id"), desc=False)
            .offset(7)
            .limit(7),
            {
                "id": [i for i in range(7, 14)],
                "name": [f"user_{i}" for i in range(7, 14)],
            },
            id="sort_asc_offset_7_limit_7",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .sort(by=col("id"), desc=True)
                .offset(7)
                .limit(17)
                .offset(5)
                .limit(11)
                .offset(7)
            ),
            {
                "id": [i for i in range(1004, 1000, -1)],
                "name": [f"user_{i}" for i in range(1004, 1000, -1)],
            },
            id="sort_desc_offset_7_limit_17_offset_5_limit_11_offset_7",
        ),
        pytest.param(
            lambda input_df: (
                input_df.select("id", "name")
                .sort(by=col("id"), desc=True)
                .filter(col("id") > 1004)
                .offset(7)
                .limit(17)
                .offset(5)
                .limit(11)
                .offset(7)
            ),
            {"id": [], "name": []},
            id="sort_desc_filter_gt_1004_offset_7_limit_17_offset_5_limit_11_offset_7",
        ),
        pytest.param(
            lambda input_df: (
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
            ),
            {"name": [f"user_{i}" for i in range(996, 993, -1)]},
            id="limit_1024_sort_desc_offset_7_offset_17_limit_15_limit_12_limit_6_offset_3",
        ),
        pytest.param(
            lambda input_df: (
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
            ),
            {"name": [f"user_{i}" for i in range(496, 493, -1)]},
            id="limit_1024_sort_desc_filter_le_523_offset_7_offset_17_limit_15_limit_12_limit_6_offset_3",
        ),
    ],
)
def test_limit_after_offset_with_sort(input_df, df_fn, expected_result):
    df = df_fn(input_df)
    if isinstance(expected_result, int):
        assert df.count_rows() == expected_result
    else:
        assert df.to_pydict() == expected_result


def test_limit_after_offset_with_sort_loop_1(input_df):
    # 516..522 -> 519..522
    df = input_df.select("id", "name").sort(by=col("id"), desc=False)
    for i in range(1, 517):
        df = df.offset(1).select("name")
    df = df.limit(15).limit(12).limit(6).offset(3)
    assert df.to_pydict() == {"name": [f"user_{i}" for i in range(519, 522)]}


def test_limit_after_offset_with_sort_loop_2(input_df):
    # 516..518 -> 519..518
    df = (
        input_df.select("id", "name")
        .sort(by=col("id"), desc=False)
        .filter(col("id") < 519)
    )
    for i in range(1, 517):
        df = df.offset(1).select("name")
    df = df.limit(15).limit(12).limit(6).offset(3)
    assert df.to_pydict() == {"name": []}


def test_paging(input_df):
    offset = 0
    limit = 100

    total = input_df.count_rows()
    while offset < total:
        paged_df = (
            input_df.select("id", "name")
            .sort(by=col("id"), desc=False)
            .offset(offset)
            .limit(limit)
        )

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
    assert sorted(df["id"]) == [i for i in range(901, 1000)]
