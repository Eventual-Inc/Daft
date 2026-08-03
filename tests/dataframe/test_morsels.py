from __future__ import annotations

import io
import os
import re

import pytest

import daft
from tests.conftest import get_tests_daft_runner_name
from tests.utils import clean_explain_output

pytestmark = pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="requires Native Runner to be in use")

NOOP_QUAL = "tests.dataframe.test_morsels.make_noop_udf.<locals>.noop"
CONCURRENCY = os.cpu_count()


def make_noop_udf(batch_size: int, dtype: daft.DataType = daft.DataType.int64()):
    @daft.func.batch(return_dtype=dtype, batch_size=batch_size)
    def noop(x: dtype) -> dtype:
        return x

    return noop


def _extract_noop_ids(plan_text):
    return re.findall(r"noop-\d+-\d+", plan_text)


def _noop_func_id(noop_id):
    return NOOP_QUAL + noop_id.removeprefix("noop")


# TODO: Add snapshot tests in Rust for the explain output of the following tests.


@pytest.mark.parametrize("dynamic_batching", [True, False])
def test_batch_size_from_udf_propagated_to_scan(dynamic_batching):
    with daft.execution_config_ctx(enable_dynamic_batching=dynamic_batching):
        df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})
        df = df.select(make_noop_udf(10)(daft.col("a")))
        string_io = io.StringIO()
        df.explain(True, file=string_io)
        captured = string_io.getvalue().split("== Physical Plan ==")[-1]
        [noop_id] = _extract_noop_ids(captured)
        expected = f"""

    * UDF {NOOP_QUAL}:
    |   Expr = {_noop_func_id(noop_id)}(col(0: a)) as a
    |   Passthrough Columns = []
    |   Properties = {{ batch_size = 10, concurrency = {CONCURRENCY}, on_error = raise, async = false, scalar = false }}
    |   Resource request = {{ num_gpus = 0 }}
    |   Stats = {{ Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }}
    |   Batch Size = 10
    |
    * InMemoryScan:
    |   Schema = a#Int64
    |   Size bytes = 40
    |   Stats = {{ Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }}
    |   Batch Size = 10

    """
        assert clean_explain_output(captured) == clean_explain_output(expected)


def test_batch_size_from_udf_propagated_through_ops_to_scan():
    df = daft.from_pydict(
        {
            "data": [
                "https://www.google.com",
                "https://www.yahoo.com",
                "https://www.bing.com",
                "https://www.duckduckgo.com",
                "https://www.ask.com",
            ]
        }
    )
    df = df.select(daft.functions.decode_image(daft.functions.download(daft.col("data"))))
    df = df.select(make_noop_udf(10, daft.DataType.image())(daft.col("data")))
    string_io = io.StringIO()
    df.explain(True, file=string_io)
    captured = string_io.getvalue().split("== Physical Plan ==")[-1]
    # Match the id inside col(0: id-...)
    m = re.search(r"col\(0: (id-[a-f0-9\-]+)\)", captured)
    id_placeholder = m.group(1) if m else ""
    [noop_id] = _extract_noop_ids(captured)
    expected = f"""

* UDF {NOOP_QUAL}:
|   Expr = {_noop_func_id(noop_id)}(col(0: __TruncateRootUDF_0-0-0__)) as data
|   Passthrough Columns = []
|   Properties = {{ batch_size = 10, concurrency = {CONCURRENCY}, on_error = raise, async = false, scalar = false }}
|   Resource request = {{ num_gpus = 0 }}
|   Stats = {{ Approx num rows = 5, Approx size bytes = 156 B, Accumulated selectivity = 1.00 }}
|   Batch Size = 10
|
* Project: col(0: __TruncateRootUDF_0-0-0__) as __TruncateRootUDF_0-0-0__
|   Stats = {{ Approx num rows = 5, Approx size bytes = 156 B, Accumulated selectivity = 1.00 }}
|   Batch Size = Range(0, 10]
|
* Project: image_decode(col(0: {id_placeholder}), lit("raise"), lit(PyObject(RGB))) as __TruncateRootUDF_0-0-0__, col(1: data)
|   Stats = {{ Approx num rows = 5, Approx size bytes = 156 B, Accumulated selectivity = 1.00 }}
|   Batch Size = Range(0, 10]
|
* Project: url_download(col(0: data), lit(true), lit("raise"), lit(32), lit(PyObject(IOConfig:
|   S3 config = {{ Max connections = 32 }}))) as {id_placeholder}, col(0: data)
|   Stats = {{ Approx num rows = 5, Approx size bytes = 156 B, Accumulated selectivity = 1.00 }}
|   Batch Size = Range(0, 10]
|
* InMemoryScan:
|   Schema = data#Utf8
|   Size bytes = 156
|   Stats = {{ Approx num rows = 5, Approx size bytes = 156 B, Accumulated selectivity = 1.00 }}
|   Batch Size = Range(0, 10]

"""
    assert clean_explain_output(captured) == clean_explain_output(expected)


def test_batch_size_from_multiple_udfs_do_not_override_each_other():
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})
    df = df.select(make_noop_udf(10)(daft.col("a")))
    df = df.select(make_noop_udf(20)(daft.col("a")))
    df = df.select(make_noop_udf(30)(daft.col("a")))
    string_io = io.StringIO()
    df.explain(True, file=string_io)
    captured = string_io.getvalue().split("== Physical Plan ==")[-1]
    noop_ids = _extract_noop_ids(captured)
    expected = f"""

* UDF {NOOP_QUAL}:
|   Expr = {_noop_func_id(noop_ids[0])}(col(0: __TruncateRootUDF_0-0-0__)) as a
|   Passthrough Columns = []
|   Properties = {{ batch_size = 30, concurrency = {CONCURRENCY}, on_error = raise, async = false, scalar = false }}
|   Resource request = {{ num_gpus = 0 }}
|   Stats = {{ Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }}
|   Batch Size = 30
|
* UDF {NOOP_QUAL}:
|   Expr = {_noop_func_id(noop_ids[1])}(col(0: __TruncateRootUDF_1-0-0__)) as __TruncateRootUDF_0-0-0__
|   Passthrough Columns = []
|   Properties = {{ batch_size = 20, concurrency = {CONCURRENCY}, on_error = raise, async = false, scalar = false }}
|   Resource request = {{ num_gpus = 0 }}
|   Stats = {{ Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }}
|   Batch Size = 20
|
* UDF {NOOP_QUAL}:
|   Expr = {_noop_func_id(noop_ids[2])}(col(0: a)) as __TruncateRootUDF_1-0-0__
|   Passthrough Columns = []
|   Properties = {{ batch_size = 10, concurrency = {CONCURRENCY}, on_error = raise, async = false, scalar = false }}
|   Resource request = {{ num_gpus = 0 }}
|   Stats = {{ Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }}
|   Batch Size = 10
|
* InMemoryScan:
|   Schema = a#Int64
|   Size bytes = 40
|   Stats = {{ Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }}
|   Batch Size = 10

"""
    assert clean_explain_output(captured) == clean_explain_output(expected)


def test_batch_size_from_udf_not_propagated_through_agg():
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})
    df = df.groupby("a").sum()
    df = df.select(make_noop_udf(10)(daft.col("a")))
    string_io = io.StringIO()
    df.explain(True, file=string_io)
    captured = string_io.getvalue().split("== Physical Plan ==")[-1]
    [noop_id] = _extract_noop_ids(captured)
    expected = f"""

* UDF {NOOP_QUAL}:
|   Expr = {_noop_func_id(noop_id)}(col(0: a)) as a
|   Passthrough Columns = []
|   Properties = {{ batch_size = 10, concurrency = {CONCURRENCY}, on_error = raise, async = false, scalar = false }}
|   Resource request = {{ num_gpus = 0 }}
|   Stats = {{ Approx num rows = 4, Approx size bytes = 32 B, Accumulated selectivity = 0.80 }}
|   Batch Size = 10
|
* GroupedAggregate:
|   Group by: col(0: a)
|   Stats = {{ Approx num rows = 4, Approx size bytes = 32 B, Accumulated selectivity = 0.80 }}
|
* InMemoryScan:
|   Schema = a#Int64
|   Size bytes = 40
|   Stats = {{ Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }}
|   Batch Size = Range(0, 131072]

"""

    assert clean_explain_output(captured) == clean_explain_output(expected)


def test_batch_size_from_udf_not_propagated_through_join():
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})
    df2 = daft.from_pydict({"b": [1, 2, 3, 4, 5]})
    df = df.join(df2, left_on="a", right_on="b")
    df = df.select(make_noop_udf(10)(daft.col("a")))
    string_io = io.StringIO()
    df.explain(True, file=string_io)
    captured = string_io.getvalue().split("== Physical Plan ==")[-1]
    [noop_id] = _extract_noop_ids(captured)
    expected = f"""

* UDF {NOOP_QUAL}:
|   Expr = {_noop_func_id(noop_id)}(col(0: a)) as a
|   Passthrough Columns = []
|   Properties = {{ batch_size = 10, concurrency = {CONCURRENCY}, on_error = raise, async = false, scalar = false }}
|   Resource request = {{ num_gpus = 0 }}
|   Stats = {{ Approx num rows = 5, Approx size bytes = 37 B, Accumulated selectivity = 0.90 }}
|   Batch Size = 10
|
* Project: col(0: a)
|   Stats = {{ Approx num rows = 5, Approx size bytes = 37 B, Accumulated selectivity = 0.90 }}
|   Batch Size = Range(0, 10]
|
* HashJoin(Inner):
|   Build on left: true
|   Track Indices: true
|   Key Schema: a#Int64
|   Null equals Nulls = [false]
|   Stats = {{ Approx num rows = 5, Approx size bytes = 37 B, Accumulated selectivity = 0.90 }}
|   Batch Size = Range(0, 10]
|\
| * Filter: not(is_null(col(0: b)))
| |   Stats = {{ Approx num rows = 5, Approx size bytes = 38 B, Accumulated selectivity = 0.95 }}
| |   Batch Size = Range(0, 10]
| |
| * InMemoryScan:
| |   Schema = b#Int64
| |   Size bytes = 40
| |   Stats = {{ Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }}
| |   Batch Size = Range(0, 10]
|
| * Filter: not(is_null(col(0: a)))
| |   Stats = {{ Approx num rows = 5, Approx size bytes = 38 B, Accumulated selectivity = 0.95 }}
| |   Batch Size = Range(0, 131072]
| |
| * InMemoryScan:
| |   Schema = a#Int64
| |   Size bytes = 40
| |   Stats = {{ Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }}
| |   Batch Size = Range(0, 131072]

"""
    assert clean_explain_output(captured) == clean_explain_output(expected)


def test_batch_size_from_into_batches():
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})
    df = df.into_batches(10)
    string_io = io.StringIO()
    df.explain(True, file=string_io)
    expected = """

* IntoBatches: 10
|   Stats = { Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }
|   Batch Size = Range(8, 10]
|
* InMemoryScan:
|   Schema = a#Int64
|   Size bytes = 40
|   Stats = { Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }
|   Batch Size = Range(8, 10]

"""
    assert clean_explain_output(string_io.getvalue().split("== Physical Plan ==")[-1]) == clean_explain_output(expected)


def test_batch_size_consecutive_into_batches():
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})
    df = df.into_batches(10)
    df = df.into_batches(20)
    df = df.into_batches(30)
    string_io = io.StringIO()
    df.explain(True, file=string_io)
    expected = """

* IntoBatches: 30
|   Stats = { Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }
|   Batch Size = Range(24, 30]
|
* InMemoryScan:
|   Schema = a#Int64
|   Size bytes = 40
|   Stats = { Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }
|   Batch Size = Range(24, 30]

"""
    assert clean_explain_output(string_io.getvalue().split("== Physical Plan ==")[-1]) == clean_explain_output(expected)


def test_batch_size_from_into_batches_before_udf():
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})
    df = df.into_batches(10)
    df = df.select(make_noop_udf(10)(daft.col("a")))
    string_io = io.StringIO()
    df.explain(True, file=string_io)
    captured = string_io.getvalue().split("== Physical Plan ==")[-1]
    [noop_id] = _extract_noop_ids(captured)
    expected = f"""

* UDF {NOOP_QUAL}:
|   Expr = {_noop_func_id(noop_id)}(col(0: a)) as a
|   Passthrough Columns = []
|   Properties = {{ batch_size = 10, concurrency = {CONCURRENCY}, on_error = raise, async = false, scalar = false }}
|   Resource request = {{ num_gpus = 0 }}
|   Stats = {{ Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }}
|   Batch Size = 10
|
* IntoBatches: 10
|   Stats = {{ Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }}
|   Batch Size = Range(8, 10]
|
* InMemoryScan:
|   Schema = a#Int64
|   Size bytes = 40
|   Stats = {{ Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }}
|   Batch Size = Range(8, 10]

"""
    assert clean_explain_output(captured) == clean_explain_output(expected)
