from __future__ import annotations

import io

import pytest

import daft
from daft import col, get_or_infer_runner_type, udf
from daft.dependencies import pa
from tests.conftest import get_tests_daft_runner_name
from tests.utils import clean_explain_output, explain_to_text


@pytest.fixture
def input_df(tmp_path):
    lance = pytest.importorskip("lance")
    lance.write_dataset(pa.Table.from_pydict({"id": [id for id in range(16)]}), uri=tmp_path)
    return daft.read_lance(uri=str(tmp_path))


@pytest.mark.skipif(
    condition=get_tests_daft_runner_name() == "native",
    reason="The physical plan displayed in Native and Ray mode is inconsistent.",
)
def test_explain_with_empty_scantask(input_df):
    string_io = io.StringIO()
    input_df.explain(True, file=string_io)
    expected = """

    * ScanTaskSource:
    |   Num Scan Tasks = 1
    |   Estimated Scan Bytes = 130
    |   Schema: {id#Int64}
    |   Scan Tasks: [
    |   {daft.io.lance.lance_scan:_lancedb_table_factory_function}
    |   ]

    """
    assert clean_explain_output(string_io.getvalue().split("== Physical Plan ==")[-1]) == clean_explain_output(expected)

    string_io = io.StringIO()
    input_df.limit(0).explain(True, file=string_io)
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
    assert clean_explain_output(string_io.getvalue().split("== Physical Plan ==")[-1]) == clean_explain_output(expected)


@udf(return_dtype=daft.DataType.string(), num_cpus=0.1, num_gpus=0)
def gen_email(ids):
    from faker import Faker

    fake = Faker()
    return [fake.email(domain="daft.ai") for _ in ids]


@pytest.mark.skipif(
    get_or_infer_runner_type() == "native",
    reason="Native runner doesn't support setting the ray_options parameter.",
)
def test_explain_with_ray_options(input_df):
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
    string_io = io.StringIO()
    df.explode("nested", index_column="idx").explain(True, file=string_io)
    output = string_io.getvalue()
    assert "Explode" in output
    assert "Index column = idx" in output
