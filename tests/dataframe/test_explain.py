from __future__ import annotations

import pytest

import daft
from daft.dependencies import pa
from daft.functions import format
from tests.conftest import get_tests_daft_runner_name
from tests.utils import clean_explain_output, explain_to_text


@pytest.fixture
def input_df(tmp_path):
    lance = pytest.importorskip("lance")
    lance.write_dataset(pa.Table.from_pydict({"idx": [id for id in range(16)]}), uri=tmp_path)
    return daft.read_lance(uri=str(tmp_path))


@pytest.mark.skipif(
    condition=get_tests_daft_runner_name() == "native",
    reason="The physical plan displayed in Native and Ray mode is inconsistent",
)
def test_explain_with_empty_scantask(input_df):
    expected = """
    * ScanTaskSource@1:
    |   Num Scan Tasks = 1
    |   Estimated Scan Bytes = 130
    |   Schema: {idx#Int64}
    |   Scan Tasks: [
    |   {daft.io.lance.lance_scan:_lancedb_table_factory_function}
    |   ]
    """
    assert clean_explain_output(explain_to_text(input_df, only_physical_plan=True)) == clean_explain_output(expected)

    expected = """
    * Limit@2: 0
    |
    * ScanTaskSource@1:
    |   Num Scan Tasks = 0
    |   Estimated Scan Bytes = 0
    |   Pushdowns: {limit: 0}
    |   Schema: {idx#Int64}
    |   Scan Tasks: [
    |   ]
    """
    assert clean_explain_output(explain_to_text(input_df.limit(0), only_physical_plan=True)) == clean_explain_output(
        expected
    )


@pytest.fixture(scope="session")
def small_df(tmp_path_factory):
    df = daft.range(start=0, end=1000, partitions=10)
    df = df.with_columns(
        {
            "s_name": format("user_{}", df["id"]),
            "s_email": format("user_{}@daft.ai", df["id"]),
        }
    )

    tmp_path = str(tmp_path_factory.mktemp("small"))
    df.write_parquet(tmp_path)
    return daft.read_parquet(tmp_path)


@pytest.fixture(scope="session")
def large_df(tmp_path_factory):
    df = daft.range(start=0, end=9999, partitions=100)
    df = df.with_columns(
        {
            "l_name": format("user_{}", df["id"]),
            "l_email": format("user_{}@daft.ai", df["id"]),
        }
    )

    tmp_path = str(tmp_path_factory.mktemp("large"))
    df.write_parquet(tmp_path)
    return daft.read_parquet(tmp_path)


@pytest.mark.skipif(
    condition=get_tests_daft_runner_name() == "native",
    reason="Native Runner doesn't currently support displaying the ID of the nodes participating in the Join",
)
def test_explain_with_broadcast_join(small_df, large_df):
    df = small_df.join(other=large_df, left_on="s_name", right_on="l_name", strategy="broadcast")
    expected = """
    * BroadcastJoin@4:
    |   Broadcaster: Node ID = 1, Join on = col(1: s_name)
    |   Receiver: Node ID = 3, Join on = col(1: l_name)
    |   Join type: Inner
    |   Is swapped: false
    |   Null equals nulls: [false]
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))

    df = large_df.join(other=small_df, left_on="l_name", right_on="s_name", strategy="broadcast")
    expected = """
    * BroadcastJoin@4:
    |   Broadcaster: Node ID = 3, Join on = col(1: s_name)
    |   Receiver: Node ID = 1, Join on = col(1: l_name)
    |   Join type: Inner
    |   Is swapped: true
    |   Null equals nulls: [false]
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))


@pytest.mark.skipif(
    condition=get_tests_daft_runner_name() == "native",
    reason="Native Runner doesn't currently support displaying the ID of the nodes participating in the Join",
)
def test_explain_with_cross_join(small_df, large_df):
    df = small_df.join(other=large_df, how="cross")
    expected = """
    * CrossJoin@4
    |   Left: Node ID = 1
    |   Right: Node ID = 3
    |\
    | * Project@3: col(0: id) as right.id, col(1: l_name), col(2: l_email)
    | |   Resource request = None
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))

    df = large_df.join(other=small_df, how="cross")
    expected = """
    * CrossJoin@4
    |   Left: Node ID = 1
    |   Right: Node ID = 3
    |\
    | * Project@3: col(0: id) as right.id, col(1: s_name), col(2: s_email)
    | |   Resource request = None
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))


@pytest.mark.skipif(
    condition=get_tests_daft_runner_name() == "native",
    reason="Native Runner doesn't currently support displaying the ID of the nodes participating in the Join",
)
def test_explain_with_hash_join(small_df, large_df):
    df = small_df.join(other=large_df, left_on="s_name", right_on="l_name", strategy="hash")
    expected = """
    * HashJoin@6:
    |   Left: Node ID = 4, Join on = col(1: s_name)
    |   Right: Node ID = 5, Join on = col(1: l_name)
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))

    df = large_df.join(other=small_df, left_on="l_name", right_on="s_name", strategy="hash")
    expected = """
    * HashJoin@6:
    |   Left: Node ID = 4, Join on = col(1: l_name)
    |   Right: Node ID = 5, Join on = col(1: s_name)
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))


@pytest.mark.skipif(
    condition=get_tests_daft_runner_name() == "native",
    reason="Native Runner doesn't currently support displaying the ID of the nodes participating in the Join",
)
def test_explain_with_sort_merged_join(small_df, large_df):
    df = small_df.join(other=large_df, left_on="s_name", right_on="l_name", strategy="sort_merge")
    expected = """
    * SortMergeJoin@4:
    |   Left: Node ID = 1, Join on = col(1: s_name)
    |   Right: Node ID = 3, Join on = col(1: l_name)
    |\
    | * Project@3: col(0: id) as right.id, col(1: l_name), col(2: l_email)
    | |   Resource request = None
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))

    df = large_df.join(other=small_df, left_on="l_name", right_on="s_name", strategy="sort_merge")
    expected = """
    * SortMergeJoin@4:
    |   Left: Node ID = 1, Join on = col(1: l_name)
    |   Right: Node ID = 3, Join on = col(1: s_name)
    |\
    | * Project@3: col(0: id) as right.id, col(1: s_name), col(2: s_email)
    | |   Resource request = None
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))
