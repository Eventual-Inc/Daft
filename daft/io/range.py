from __future__ import annotations

from collections.abc import Iterator
from typing import Callable

from daft import DataType
from daft.io._generator import GeneratorScanOperator
from daft.logical.schema import Schema
from daft.table.table import Table


def _range_generators(start: int, end: int, step: int) -> Iterator[Callable[[], Iterator[Table]]]:
    def generator_for_value(value: int) -> Callable[[], Iterator[Table]]:
        def generator() -> Iterator[Table]:
            yield Table.from_pydict({"id": [value]})

        return generator

    for value in range(start, end, step):
        yield generator_for_value(value)


class RangeScanOperator(GeneratorScanOperator):
    def __init__(self, start: int, end: int, step: int = 1) -> None:
        schema = Schema._from_field_name_and_types([("id", DataType.int64())])

        super().__init__(schema=schema, generators=_range_generators(start, end, step))
