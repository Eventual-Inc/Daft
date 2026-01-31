from __future__ import annotations

import os
import time

import pytest

import daft
from daft import udf
from daft.daft import dashboard


@pytest.fixture(scope="module")
def dashboard_url():
    handle = dashboard.launch(noop_if_initialized=True)
    port = handle.get_port()
    url = f"http://127.0.0.1:{port}"
    os.environ["DAFT_DASHBOARD_URL"] = url
    yield url
    handle.shutdown(noop_if_shutdown=True)


@pytest.mark.integration
def test_dashboard_ray_flotilla(dashboard_url):
    daft.set_runner_ray(address="auto")

    @udf(return_dtype=daft.DataType.int64())
    def slow_inc(x):
        time.sleep(0.1)
        return [i + 1 for i in x.to_pylist()]

    df = daft.from_pydict({"a": list(range(100))})
    df = df.repartition(5)
    df = df.with_column("b", slow_inc(df["a"]))
    df = df.repartition(2)

    result = df.collect()
    assert len(result) == 100


@pytest.mark.integration
def test_dashboard_native_swordfish(dashboard_url):
    daft.set_runner_native()

    df = daft.from_pydict({"a": list(range(100))})
    df = df.with_column("b", df["a"] + 1)
    df = df.repartition(3)

    result = df.collect()
    assert len(result) == 100
