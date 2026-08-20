from __future__ import annotations

from pathlib import Path

import pytest

import daft
from daft import col
from daft.daft import JoinStrategy, JoinType, KeyFilteringConfig
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(get_tests_daft_runner_name() != "ray", reason="requires Ray Runner to be in use")


def helper_key_filtering_anti_join(
    left: daft.DataFrame,
    right: daft.DataFrame,
    left_on: list,
    right_on: list,
) -> daft.DataFrame:
    builder = left._builder.join(
        right._builder,
        left_on=left_on,
        right_on=right_on,
        how=JoinType.Anti,
        strategy=JoinStrategy.KeyFiltering,
        key_filtering_config=KeyFilteringConfig(
            num_workers=2,
            cpus_per_worker=1.0,
            keys_load_batch_size=16,
            max_concurrency_per_worker=1,
            filter_batch_size=8,
        ),
    )
    return daft.DataFrame(builder)


def helper_file_backed_right_df(tmp_path: Path, name: str, data: dict[str, list]) -> daft.DataFrame:
    right_path = tmp_path / name
    daft.from_pydict(data).write_parquet(str(right_path))
    return daft.read_parquet(str(right_path))


def test_key_filtering_join_filters_single_key_with_distinct_right_column_name(tmp_path: Path) -> None:
    left = daft.from_pydict(
        {
            "left_id": [1, 2, 3, 4],
            "payload": ["a", "b", "c", "d"],
        }
    )
    right = helper_file_backed_right_df(
        tmp_path,
        "right_single",
        {
            "right_id": [2, 4, 5],
            "keep": [True, True, False],
        },
    ).where(col("keep"))

    result = helper_key_filtering_anti_join(
        left,
        right,
        left_on=[left["left_id"]],
        right_on=[right["right_id"]],
    ).sort("left_id")

    assert result.to_pydict() == {
        "left_id": [1, 3],
        "payload": ["a", "c"],
    }


def test_key_filtering_join_filters_composite_keys_with_distinct_column_names(tmp_path: Path) -> None:
    left = daft.from_pydict(
        {
            "left_id": [1, 1, 2, 3],
            "left_bucket": ["a", "b", "a", "a"],
            "payload": ["keep-1", "keep-2", "keep-3", "keep-4"],
        }
    )
    right = helper_file_backed_right_df(
        tmp_path,
        "right_composite",
        {
            "right_id": [1, 2, 9],
            "right_bucket": ["a", "b", "z"],
            "keep": [True, True, False],
        },
    ).where(col("keep"))

    result = helper_key_filtering_anti_join(
        left,
        right,
        left_on=[left["left_id"], left["left_bucket"]],
        right_on=[right["right_id"], right["right_bucket"]],
    ).sort(["left_id", "left_bucket"])

    assert result.to_pydict() == {
        "left_id": [1, 2, 3],
        "left_bucket": ["b", "a", "a"],
        "payload": ["keep-2", "keep-3", "keep-4"],
    }
