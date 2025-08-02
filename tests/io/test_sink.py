from __future__ import annotations

from collections.abc import Iterator

import pytest
import ray

import daft
from daft.io.sink import DataSink, WriteResult
from daft.recordbatch import MicroPartition


class UnserializableException(Exception):
    def __init__(self, message="Recovered the message in the exception"):
        super().__init__(message)
        self.unsafe_lambda = lambda x: x * 2


class SampleSink(DataSink[None]):
    def name(self) -> str:
        return "sample_sink"

    def schema(self) -> daft.Schema:
        return daft.Schema.from_pydict({"id": daft.DataType.int64()})

    def write(self, df: daft.DataFrame) -> Iterator[WriteResult[None]]:
        raise UnserializableException()

    def finalize(self, write_results: list[WriteResult[None]]) -> MicroPartition:
        result_table = MicroPartition.from_pydict(
            {
                "write_responses": write_results,
            }
        )

        return result_table


def test_sink_raises_unserializable_exception():
    df = daft.from_pydict({"id": [1, 2, 3]})
    sink = SampleSink()
    with pytest.raises(Exception) as exc_info:
        df.write_sink(sink)

    e = exc_info.value
    assert isinstance(e, (RuntimeError, ray.exceptions.RayTaskError))
    assert "UnserializableException" in str(e)
