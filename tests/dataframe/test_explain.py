from __future__ import annotations

import pytest

import daft
from daft import col, get_or_infer_runner_type, udf
from daft.dependencies import pa
from daft.functions import format
from tests.conftest import get_tests_daft_runner_name
from tests.utils import clean_explain_output, explain_to_text


@pytest.fixture
def input_df_with_uri(tmp_path):
    dataset_uri = f"{tmp_path}/test.lance"
    lance = pytest.importorskip("lance")
    lance.write_dataset(pa.Table.from_pydict({"id": [id for id in range(16)]}), uri=dataset_uri)
    return daft.read_lance(uri=dataset_uri), dataset_uri


def test_explain_with_python_function_datasource(input_df_with_uri):
    input_df, dataset_uri = input_df_with_uri
    runner_type = get_or_infer_runner_type()
    if runner_type == "native":
        expected = f"""
        * ScanTaskSource:
        |   Source = LanceDBScanOperator({dataset_uri})
        |   Num Scan Tasks = 1
        |   Estimated Scan Bytes = 130
        |   Num Parallel Scan Tasks = 1
        |   Schema: {{id#Int64}}
        |   Scan Tasks: [
        |   {{daft.io.lance.lance_scan:_lancedb_table_factory_function}}
        |   ]
        |   Stats = {{ Approx num rows = 16, Approx size bytes = 130 B, Accumulated selectivity = 1.00 }}
        |   Batch Size = Range(0, 131072]
        """
        input_df.explain(True)
        assert clean_explain_output(explain_to_text(input_df, only_physical_plan=True)) == clean_explain_output(
            expected
        )

        expected = """
        * Limit: 0
        |   Stats = { Approx num rows = 0, Approx size bytes = 0 B, Accumulated selectivity = 1.00 }
        |   Batch Size = Range(0, 131072]
        |
        * Empty Scan:
        |   Schema = id#Int64
        |   Stats = { Approx num rows = 0, Approx size bytes = 0 B, Accumulated selectivity = 1.00 }
        |   Batch Size = Range(0, 131072]
        """
        assert clean_explain_output(
            explain_to_text(input_df.limit(0), only_physical_plan=True)
        ) == clean_explain_output(expected)
    else:
        expected = f"""
        * ScanTaskSource:
        |   Source = LanceDBScanOperator({dataset_uri})
        |   Num Scan Tasks = 1
        |   Estimated Scan Bytes = 130
        |   Schema: {{id#Int64}}
        |   Scan Tasks: [
        |   {{daft.io.lance.lance_scan:_lancedb_table_factory_function}}
        |   ]
        """
        assert clean_explain_output(explain_to_text(input_df, only_physical_plan=True)) == clean_explain_output(
            expected
        )

        expected = """
        * Limit: 0
        |
        * ScanTaskSource:
        |   Num Scan Tasks = 0
        |   Estimated Scan Bytes = 0
        |   Pushdowns: {limit: 0}
        |   Schema: {id#Int64}
        |   Scan Tasks: [
        |   ]
        """
        assert clean_explain_output(
            explain_to_text(input_df.limit(0), only_physical_plan=True)
        ) == clean_explain_output(expected)


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
    * BroadcastJoin
    |   Type: Inner
    |   Left: Join key = col(1: s_name), Role = Broadcaster
    |   Right: Join key = col(1: l_name), Role = Receiver
    |   Null equals nulls: [false]
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))

    df = large_df.join(other=small_df, left_on="l_name", right_on="s_name", strategy="broadcast")
    expected = """
    * BroadcastJoin
    |   Type: Inner
    |   Left: Join key = col(1: l_name), Role = Receiver
    |   Right: Join key = col(1: s_name), Role = Broadcaster
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
    * CrossJoin
    |   Left: Node name = ScanSource
    |   Right: Node name = Project
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))

    df = large_df.join(other=small_df, how="cross")
    expected = """
    * CrossJoin
    |   Left: Node name = ScanSource
    |   Right: Node name = Project
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))


@pytest.mark.skipif(
    condition=get_tests_daft_runner_name() == "native",
    reason="Native Runner doesn't currently support displaying the ID of the nodes participating in the Join",
)
def test_explain_with_hash_join(small_df, large_df):
    df = small_df.join(other=large_df, left_on="s_name", right_on="l_name", strategy="hash", how="left")
    expected = """
    * HashJoin
    |   Type: Left
    |   Left: Join key = col(1: s_name)
    |   Right: Join key = col(1: l_name)
    |   Null equals nulls: [false]
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))

    df = large_df.join(other=small_df, left_on="l_name", right_on="s_name", strategy="hash", how="right")
    expected = """
    * HashJoin
    |   Type: Right
    |   Left: Join key = col(1: l_name)
    |   Right: Join key = col(1: s_name)
    |   Null equals nulls: [false]
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))


@pytest.mark.skipif(
    condition=get_tests_daft_runner_name() == "native",
    reason="Native Runner doesn't currently support displaying the ID of the nodes participating in the Join",
)
def test_explain_with_sort_merged_join(small_df, large_df):
    df = small_df.join(other=large_df, left_on="s_name", right_on="l_name", strategy="sort_merge")
    expected = """
    * SortMergeJoin
    |   Type: Inner
    |   Left: Join key = col(1: s_name)
    |   Right: Join key = col(1: l_name)
    |   Num partitions: 100
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))

    df = large_df.join(other=small_df, left_on="l_name", right_on="s_name", strategy="sort_merge")
    expected = """
    * SortMergeJoin
    |   Type: Inner
    |   Left: Join key = col(1: l_name)
    |   Right: Join key = col(1: s_name)
    |   Num partitions: 100
        """
    assert clean_explain_output(expected) in clean_explain_output(explain_to_text(df, only_physical_plan=True))


@udf(return_dtype=daft.DataType.string(), num_cpus=0.1, num_gpus=0)
def gen_email(ids):
    from faker import Faker

    fake = Faker()
    return [fake.email(domain="daft.ai") for _ in ids]


@pytest.mark.skipif(
    get_or_infer_runner_type() == "native",
    reason="Native runner doesn't support setting the ray_options parameter.",
)
def test_explain_with_ray_options(input_df_with_uri):
    input_df, dataset_uri = input_df_with_uri

    # Currently only supports 'conda' option
    with pytest.raises(ValueError) as exc_info:
        gen_email_udf = gen_email.override_options(
            ray_options={"runtime_env": {"conda": "daft", "working_dir": "/tmp/daft"}}
        ).with_concurrency(1)
        input_df.with_column("email", gen_email_udf(col("id"))).collect()

    value = str(exc_info.value)
    assert (
        "The conda environment is only allowed to be configured through runtime_env in ray_options, "
        "but got '{'conda': 'daft', 'working_dir': '/tmp/daft'}'"
    ) in value, f"Unexpected UDF Exception: {value}"

    # Configure via Conda Env Name
    gen_email_udf = gen_email.override_options(ray_options={"runtime_env": {"conda": "punks"}}).with_concurrency(1)
    df = input_df.with_column("email", gen_email_udf(col("id")))
    text = explain_to_text(df)
    assert "{'runtime_env': {'conda': 'punks'}}" in text, f"Unexpected Explain result: {text}"

    # Configure via Conda YAML File
    gen_email_udf = gen_email.override_options(
        ray_options={"runtime_env": {"conda": "/tmp/daft/conda_env.yaml"}}
    ).with_concurrency(1)
    df = input_df.with_column("email", gen_email_udf(col("id")))
    text = explain_to_text(df)
    assert "{'runtime_env': {'conda': '/tmp/daft/conda_env.yaml'}}" in text, f"Unexpected Explain result: {text}"

    # Configure via Conda YAML Config
    gen_email_udf = gen_email.override_options(ray_options={"runtime_env": {}}).with_concurrency(1)
    df = input_df.with_column("email", gen_email_udf(col("id")))
    text = explain_to_text(df)
    assert "{'runtime_env': {}}" in text, f"Unexpected Explain result: {text}"

    gen_email_udf = gen_email.override_options(
        ray_options={
            "runtime_env": {
                "conda": {
                    "name": "simple",
                    "channels": ["conda-forge"],
                }
            }
        }
    ).with_concurrency(1)

    df = input_df.with_column("email", gen_email_udf(col("id")))
    text = explain_to_text(df)
    assert "{'runtime_env': {'conda': {'name': 'simple', 'channels': ['conda-forge']}}}" in text, (
        f"Unexpected Explain result: {text}"
    )


def test_explain_with_explode_index_column():
    df = daft.from_pydict({"nested": [[1, 2], [3, 4]]})
    df = df.explode("nested", index_column="idx")
    output = explain_to_text(df)
    assert "Explode" in output
    assert "Index column = idx" in output
