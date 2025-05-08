import os

# These tests simply ensure that the opentelemetry does not cause errors when enabled
os.environ["DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT"] = "grpc://localhost:4317"

import daft


def test_count_star() -> None:
    df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [1, None, 3, None]})
    res = df.count("*").to_pydict()
    assert res == {"count": [4]}
