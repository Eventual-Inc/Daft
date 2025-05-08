import importlib
import os

import pytest

import daft


@pytest.fixture
def ensure_otel_enabled():
    original_value = os.environ.get("DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT")
    os.environ["DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT"] = "grpc://localhost:4317"

    yield

    if original_value is not None:
        os.environ["DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT"] = original_value
    else:
        os.environ.pop("DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT", None)


def test_count_star(ensure_otel_enabled) -> None:
    importlib.reload(daft)
    df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [1, None, 3, None]})
    res = df.count("*").to_pydict()
    assert res == {"count": [4]}
