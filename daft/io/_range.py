from __future__ import annotations

from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from collections.abc import Iterator

from daft import DataType
from daft.io._generator import GeneratorScanOperator
from daft.logical.schema import Schema
from daft.recordbatch.recordbatch import RecordBatch


def _range_generators(
    start: int, end: int, step: int, partitions: int
) -> Iterator[Callable[[], Iterator[RecordBatch]]]:
    # TODO: Partitioning with range scan is currently untested and unused.
    # There may be issues with balanced partitions and step size.

    # Calculate partition bounds upfront
    partition_size = (end - start) // partitions
    partition_bounds = [
        (start + (i * partition_size), start + ((i + 1) * partition_size) if i < partitions - 1 else end)
        for i in range(partitions)
    ]

    def generator(partition_idx: int) -> Iterator[RecordBatch]:
        partition_start, partition_end = partition_bounds[partition_idx]
        values = list(range(partition_start, partition_end, step))
        yield RecordBatch.from_pydict({"id": values})

    from functools import partial

    for partition_idx in range(partitions):
        yield partial(generator, partition_idx)


class RangeScanOperator(GeneratorScanOperator):
    def __init__(self, start: int, end: int, step: int = 1, partitions: int = 1) -> None:
        schema = Schema._from_field_name_and_types([("id", DataType.int64())])

        super().__init__(schema=schema, generators=_range_generators(start, end, step, partitions))
