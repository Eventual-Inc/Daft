from __future__ import annotations

import io
import re

import pytest

import daft
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(get_tests_daft_runner_name() != "native", reason="requires Native Runner to be in use")

_pattern = re.compile("|".join(map(re.escape, ["\n", "|", " ", "*", "\\"])))
_rep = {"\n": "", "|": "", " ": "", "*": "", "\\": ""}


def clean_explain_output(output: str) -> str:
    output = _pattern.sub(lambda m: _rep[m.group(0)], output)
    return output.strip()


def make_noop_udf(batch_size: int, dtype: daft.DataType = daft.DataType.int64()):
    @daft.udf(return_dtype=dtype, batch_size=batch_size, concurrency=1)
    def noop(x: dtype) -> dtype:
        return x

    return noop


# TODO: Add snapshot tests in Rust for the explain output of the following tests.


def test_batch_size_from_udf_propagated_to_scan():
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})
    df = df.select(make_noop_udf(10)(daft.col("a")))
    string_io = io.StringIO()
    df.explain(True, file=string_io)
    expected = """

* UDF Executor:
|   UDF tests.dataframe.test_morsels.make_noop_udf.<locals>.noop = py_udf(col(0: a)) as a
|   Passthrough Columns = []
|   Concurrency = 1
|   Resource request = None
|   Stats = { Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }
|   Batch Size = 10
|
* InMemorySource:
|   Schema = a#Int64
|   Size bytes = 40
|   Stats = { Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }
|   Batch Size = 10

"""
    assert clean_explain_output(string_io.getvalue().split("== Physical Plan ==")[-1]) == clean_explain_output(expected)


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
    df = df.select(daft.col("data").url.download().image.decode())
    df = df.select(make_noop_udf(10, daft.DataType.image())(daft.col("data")))
    string_io = io.StringIO()
    df.explain(True, file=string_io)
    captured = string_io.getvalue().split("== Physical Plan ==")[-1]
    # Match the id inside col(0: id-...)
    m = re.search(r"col\(0: (id-[a-f0-9\-]+)\)", captured)
    id_placeholder = m.group(1) if m else ""
    expected = f"""

* UDF Executor:
|   UDF tests.dataframe.test_morsels.make_noop_udf.<locals>.noop = py_udf(col(0: __TruncateRootUDF_0-0-0__)) as data
|   Passthrough Columns = []
|   Concurrency = 1
|   Resource request = None
|   Batch Size = 10
|
* Project: col(0: __TruncateRootUDF_0-0-0__) as __TruncateRootUDF_0-0-0__
|   Batch Size = Range(0, 10]
|
* Project: image_decode(col(0: {id_placeholder}), lit("raise"), lit(Null)) as __TruncateRootUDF_0-0-0__, col(1: data)
|   Batch Size = Range(0, 10]
|
* Project: url_download(col(0: data), lit(true), lit("raise"), lit(32), lit(PyObject(IOConfig:
|   S3Config
|       region_name: None
|       endpoint_url: None
|       key_id: None
|       session_token: None,
|       access_key: None
|       credentials_provider: None
|       buffer_time: None
|       max_connections: 32,
|       retry_initial_backoff_ms: 1000,
|       connect_timeout_ms: 30000,
|       read_timeout_ms: 30000,
|       num_tries: 25,
|       retry_mode: Some("adaptive"),
|       anonymous: false,
|       use_ssl: true,
|       verify_ssl: true,
|       check_hostname_ssl: true
|       requester_pays: false
|       force_virtual_addressing: false
|   AzureConfig
|       storage_account: None
|       access_key: None
|       sas_token: None
|       bearer_token: None
|       tenant_id: None
|       client_id: None
|       client_secret: None
|       use_fabric_endpoint: false
|       anonymous: false
|       endpoint_url: None
|       use_ssl: true
|   GCSConfig
|       project_id: None
|       anonymous: false
|       max_connections_per_io_thread: 8
|       retry_initial_backoff_ms: 1000
|       connect_timeout_ms: 30000
|       read_timeout_ms: 30000
|       num_tries: 5
|   HTTPConfig
|   User agent = daft/0.0.1
|   Retry initial backoff ms = 1000
|   Connect timeout ms = 30000
|   Read timeout ms = 30000
|   Max retries = 5))) as {id_placeholder}, col(0: data)
|   Batch Size = Range(0, 10]
|
* InMemorySource:
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
    expected = """

* UDF Executor:
|   UDF tests.dataframe.test_morsels.make_noop_udf.<locals>.noop = py_udf(col(0: __TruncateRootUDF_0-0-0__)) as a
|   Passthrough Columns = []
|   Concurrency = 1
|   Resource request = None
|   Stats = { Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }
|   Batch Size = 30
|
* UDF Executor:
|   UDF tests.dataframe.test_morsels.make_noop_udf.<locals>.noop = py_udf(col(0: __TruncateRootUDF_1-0-0__)) as __TruncateRootUDF_0-0-0__
|   Passthrough Columns = []
|   Concurrency = 1
|   Resource request = None
|   Stats = { Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }
|   Batch Size = 20
|
* UDF Executor:
|   UDF tests.dataframe.test_morsels.make_noop_udf.<locals>.noop = py_udf(col(0: a)) as __TruncateRootUDF_1-0-0__
|   Passthrough Columns = []
|   Concurrency = 1
|   Resource request = None
|   Stats = { Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }
|   Batch Size = 10
|
* InMemorySource:
|   Schema = a#Int64
|   Size bytes = 40
|   Stats = { Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }
|   Batch Size = 10

"""
    assert clean_explain_output(string_io.getvalue().split("== Physical Plan ==")[-1]) == clean_explain_output(expected)


def test_batch_size_from_udf_not_propagated_through_agg():
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})
    df = df.groupby("a").sum()
    df = df.select(make_noop_udf(10)(daft.col("a")))
    string_io = io.StringIO()
    df.explain(True, file=string_io)
    expected = """

* UDF Executor:
|   UDF tests.dataframe.test_morsels.make_noop_udf.<locals>.noop = py_udf(col(0: a)) as a
|   Passthrough Columns = []
|   Concurrency = 1
|   Resource request = None
|   Stats = { Approx num rows = 4, Approx size bytes = 32 B, Accumulated selectivity = 0.80 }
|   Batch Size = 10
|
* GroupedAggregate:
|   Group by: col(0: a)
|   Stats = { Approx num rows = 4, Approx size bytes = 32 B, Accumulated selectivity = 0.80 }
|   Batch Size = Range(0, 131072]
|
* InMemorySource:
|   Schema = a#Int64
|   Size bytes = 40
|   Stats = { Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }
|   Batch Size = Range(0, 131072]

"""

    assert clean_explain_output(string_io.getvalue().split("== Physical Plan ==")[-1]) == clean_explain_output(expected)


def test_batch_size_from_udf_not_propagated_through_join():
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})
    df2 = daft.from_pydict({"b": [1, 2, 3, 4, 5]})
    df = df.join(df2, left_on="a", right_on="b")
    df = df.select(make_noop_udf(10)(daft.col("a")))
    string_io = io.StringIO()
    df.explain(True, file=string_io)
    expected = """

* UDF Executor:
|   UDF tests.dataframe.test_morsels.make_noop_udf.<locals>.noop = py_udf(col(0: a)) as a
|   Passthrough Columns = []
|   Concurrency = 1
|   Resource request = None
|   Stats = { Approx num rows = 5, Approx size bytes = 37 B, Accumulated selectivity = 0.90 }
|   Batch Size = 10
|
* Project: col(0: a)
|   Stats = { Approx num rows = 5, Approx size bytes = 37 B, Accumulated selectivity = 0.90 }
|   Batch Size = Range(0, 10]
|
* InnerHashJoinProbe:
|   Probe on: [col(0: b)]
|   Build on left: true
|   Stats = { Approx num rows = 5, Approx size bytes = 37 B, Accumulated selectivity = 0.90 }
|   Batch Size = Range(0, 10]
|\
| * Filter: not(is_null(col(0: b)))
| |   Stats = { Approx num rows = 5, Approx size bytes = 38 B, Accumulated selectivity = 0.95 }
| |   Batch Size = Range(0, 10]
| |
| * InMemorySource:
| |   Schema = b#Int64
| |   Size bytes = 40
| |   Stats = { Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }
| |   Batch Size = Range(0, 10]
|
* HashJoinBuild:
|   Track Indices: true
|   Key Schema: a#Int64
|   Null equals Nulls = [false]
|   Stats = { Approx num rows = 5, Approx size bytes = 38 B, Accumulated selectivity = 0.95 }
|   Batch Size = Range(0, 131072]
|
* Filter: not(is_null(col(0: a)))
|   Stats = { Approx num rows = 5, Approx size bytes = 38 B, Accumulated selectivity = 0.95 }
|   Batch Size = Range(0, 131072]
|
* InMemorySource:
|   Schema = a#Int64
|   Size bytes = 40
|   Stats = { Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }
|   Batch Size = Range(0, 131072]

"""
    assert clean_explain_output(string_io.getvalue().split("== Physical Plan ==")[-1]) == clean_explain_output(expected)


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
* InMemorySource:
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
* InMemorySource:
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
    print(string_io.getvalue())
    expected = """

* UDF Executor:
|   UDF tests.dataframe.test_morsels.make_noop_udf.<locals>.noop = py_udf(col(0: a)) as a
|   Passthrough Columns = []
|   Concurrency = 1
|   Resource request = None
|   Stats = { Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }
|   Batch Size = 10
|
* IntoBatches: 10
|   Stats = { Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }
|   Batch Size = Range(8, 10]
|
* InMemorySource:
|   Schema = a#Int64
|   Size bytes = 40
|   Stats = { Approx num rows = 5, Approx size bytes = 40 B, Accumulated selectivity = 1.00 }
|   Batch Size = Range(8, 10]

"""
    assert clean_explain_output(string_io.getvalue().split("== Physical Plan ==")[-1]) == clean_explain_output(expected)
