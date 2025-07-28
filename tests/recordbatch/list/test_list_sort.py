from __future__ import annotations

import daft
from daft.datatype import DataType
from daft.expressions import col
from daft.recordbatch import MicroPartition


def test_list_sort():
    table = MicroPartition.from_pydict(
        {
            "a": [[1, 3], [4, 6, 2], [3], [11, 6], None, [], [1, None, 3], None],
            "b": [True, False, False, True, False, False, False, True],
        }
    ).eval_expression_list([col("a").cast(DataType.list(DataType.int64())), col("b")])

    res = table.eval_expression_list(
        [
            col("a").list.sort().alias("asc"),
            col("a").list.sort(True).alias("desc"),
            col("a").list.sort(col("b")).alias("mixed"),
        ]
    )

    assert res.to_pydict() == {
        "asc": [[1, 3], [2, 4, 6], [3], [6, 11], None, [], [1, 3, None], None],
        "desc": [[3, 1], [6, 4, 2], [3], [11, 6], None, [], [None, 3, 1], None],
        "mixed": [[3, 1], [2, 4, 6], [3], [11, 6], None, [], [1, 3, None], None],
    }


def test_list_sort_fixed_size():
    table = MicroPartition.from_pydict(
        {
            "a": [[1, 3], [6, 2], [3, 3], [11, 6], None, [2, None], None],
            "b": [True, False, False, True, False, False, True],
        }
    ).eval_expression_list([col("a").cast(DataType.fixed_size_list(DataType.int64(), 2)), col("b")])

    res = table.eval_expression_list(
        [
            col("a").list.sort().alias("asc"),
            col("a").list.sort(True).alias("desc"),
            col("a").list.sort(col("b")).alias("mixed"),
        ]
    )

    assert res.to_pydict() == {
        "asc": [[1, 3], [2, 6], [3, 3], [6, 11], None, [2, None], None],
        "desc": [[3, 1], [6, 2], [3, 3], [11, 6], None, [None, 2], None],
        "mixed": [[3, 1], [2, 6], [3, 3], [11, 6], None, [2, None], None],
    }


def test_list_sort_with_groupby():
    df = daft.from_pydict({"group_col": [1, 1, 1, 2, 2, 2], "id_col": ["c", "a", "b", "e", "d", "a"]})

    # Group by group_col, aggregate id_col as a list.
    grouped_df = df.groupby("group_col").agg(daft.col("id_col").agg_list().alias("ids_col"))

    # Sort the list.
    result = grouped_df.select(col("group_col"), col("ids_col").list.sort()).sort("group_col", desc=False)
    result_dict = result.to_pydict()
    expected = {"group_col": [1, 2], "ids_col": [["a", "b", "c"], ["a", "d", "e"]]}
    assert result_dict == expected

    # Cast to fixed size list and sort.
    result = grouped_df.select(
        col("group_col"), col("ids_col").cast(DataType.fixed_size_list(DataType.string(), 3)).list.sort()
    ).sort("group_col", desc=False)
    result_dict = result.to_pydict()
    expected = {"group_col": [1, 2], "ids_col": [["a", "b", "c"], ["a", "d", "e"]]}
    assert result_dict == expected


# Reproduce issue #4862.
def test_list_sort_groupby_larger_than_morsel_size():
    # Test that reproduces a bug with large datasets and groupby operations that exceed the default morsel size.This would previously fail with offset mismatch errors.
    import itertools

    morsel_size = daft.context.get_context().daft_execution_config.default_morsel_size
    num_groups = int(morsel_size + 10)
    expected_records_per_group = 1
    num_records = num_groups * expected_records_per_group

    group_ids = list(itertools.chain(*[expected_records_per_group * [f"{i}"] for i in range(num_groups)]))
    record_ids = [f"r-{i}" for i in range(num_records)]

    result = (
        daft.from_pydict(
            {
                "group_id": group_ids,
                "record_id": record_ids,
            }
        )
        .groupby("group_id")
        .agg(daft.col("record_id").agg_list().alias("record_ids"))
        .with_column(
            "record_ids_key",
            daft.col("record_ids").list.sort(),
        )
        .select(
            "group_id",
            "record_ids_key",
        )
    )

    result_dict = result.to_pydict()

    assert (
        len(result_dict["record_ids_key"]) == num_groups
    ), f"Expected {num_groups} groups, got {len(result_dict['record_ids_key'])}"

    # Each group should have exactly expected_records_per_group records: Group i gets records r-i*expected_records to r-(i+1)*expected_records-1
    for group_id, record_list in zip(result_dict["group_id"], result_dict["record_ids_key"]):
        assert (
            len(record_list) == expected_records_per_group
        ), f"Group {group_id} should have {expected_records_per_group} records, got {len(record_list)}"

        start_record = int(group_id) * expected_records_per_group
        expected_sorted = sorted([f"r-{start_record + i}" for i in range(expected_records_per_group)])
        assert (
            record_list == expected_sorted
        ), f"Group {group_id} should be sorted: {record_list[:]}... vs {expected_sorted[:]}..."
