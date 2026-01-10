from __future__ import annotations

import pytest

from daft.daft import JoinType
from daft.expressions import col
from daft.recordbatch.recordbatch import RecordBatch


def _rb_from_dict(d: dict[str, list]):
    return RecordBatch.from_pydict(d)


@pytest.mark.parametrize("is_sorted", [False, True])
def test_recordbatch_sort_merge_join_inner_is_sorted_branch(is_sorted: bool) -> None:
    # Use identical join key names on both sides to trigger coalescing of common join columns
    left = _rb_from_dict({"x": [3, 1, 2], "lx": [30, 10, 20]})
    right = _rb_from_dict({"x": [2, 3, 4], "ry": [200, 300, 400]})

    if is_sorted:
        left = left.sort([col("x")])
        right = right.sort([col("x")])

    out = left.sort_merge_join(right, left_on=[col("x")], right_on=[col("x")], how=JoinType.Inner, is_sorted=is_sorted)

    # Expect only matched rows x=2 and x=3; common join column "x" is coalesced into a single column
    expected = {
        "x": [2, 3],
        "lx": [20, 30],
        "ry": [200, 300],
    }
    out_sorted = out.sort([col("x")])
    assert out_sorted.to_pydict() == expected


def test_recordbatch_sort_merge_join_unsupported_join_type() -> None:
    left = _rb_from_dict({"x": [1, 2], "lx": [10, 20]})
    right = _rb_from_dict({"x": [1, 3], "ry": [100, 300]})

    with pytest.raises(NotImplementedError, match="Implement Other Join types"):
        # Left/Right/Outer are not supported for sort_merge_join yet and should raise at the Python wrapper layer
        left.sort_merge_join(right, left_on=[col("x")], right_on=[col("x")], how=JoinType.Left, is_sorted=False)


def test_recordbatch_sort_merge_join_no_columns_error() -> None:
    left = _rb_from_dict({"x": [1, 2], "lx": [10, 20]})
    right = _rb_from_dict({"x": [1, 2], "ry": [100, 200]})

    # Passing empty join keys should raise ValueError from Rust: "No columns were passed in to join on"
    with pytest.raises(ValueError, match="No columns were passed in to join on"):
        left.sort_merge_join(right, left_on=[], right_on=[], how=JoinType.Inner, is_sorted=False)


def test_recordbatch_sort_merge_join_semi_anti_is_sorted_false_true() -> None:
    left = _rb_from_dict({"x": [4, 2, 3, 1], "lx": [40, 20, 30, 10]})
    right = _rb_from_dict({"x": [2, 3]})

    # Semi: keep only keys that exist on the right side
    semi_false = left.sort_merge_join(
        right, left_on=[col("x")], right_on=[col("x")], how=JoinType.Semi, is_sorted=False
    )
    semi_true = left.sort([col("x")]).sort_merge_join(
        right.sort([col("x")]), left_on=[col("x")], right_on=[col("x")], how=JoinType.Semi, is_sorted=True
    )

    assert semi_false.sort([col("x")]).to_pydict()["x"] == [2, 3]
    assert semi_true.sort([col("x")]).to_pydict()["x"] == [2, 3]

    # Anti: keep only keys that do not exist on the right side
    anti_false = left.sort_merge_join(
        right, left_on=[col("x")], right_on=[col("x")], how=JoinType.Anti, is_sorted=False
    )
    anti_true = left.sort([col("x")]).sort_merge_join(
        right.sort([col("x")]), left_on=[col("x")], right_on=[col("x")], how=JoinType.Anti, is_sorted=True
    )

    assert anti_false.sort([col("x")]).to_pydict()["x"] == [1, 4]
    assert anti_true.sort([col("x")]).to_pydict()["x"] == [1, 4]
