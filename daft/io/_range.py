from __future__ import annotations

from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from collections.abc import Iterator

from daft import DataType
from daft.io._generator import GeneratorScanOperator
from daft.logical.schema import Schema
from daft.recordbatch.recordbatch import RecordBatch
from daft.api_annotations import PublicAPI
from daft.dataframe import DataFrame
from typing import Union, Optional, overload
from daft.io.scan import ScanOperator
from daft.daft import ScanOperatorHandle
from daft.logical.builder import LogicalPlanBuilder

@PublicAPI
@overload
def _range(end: int) -> DataFrame:
    """Creates a DataFrame with a range of values.

    The range starts at 0 and ends at `end` (exclusive) with a step size of 1.

    Example:
        >>> import daft
        >>> daft.range(5).show()
        ╭───────╮
        │ id    │
        │ ---   │
        │ Int64 │
        ╞═══════╡
        │ 0     │
        ├╌╌╌╌╌╌╌┤
        │ 1     │
        ├╌╌╌╌╌╌╌┤
        │ 2     │
        ├╌╌╌╌╌╌╌┤
        │ 3     │
        ├╌╌╌╌╌╌╌┤
        │ 4     │
        ╰───────╯
        <BLANKLINE>
        (Showing first 5 of 5 rows)
    """
    ...
@PublicAPI
@overload
def _range(start: int, end: int) -> DataFrame:
    ...
    """Creates a DataFrame with a range of values.

    The range starts at `start` and ends at `end` (exclusive) with a step size of 1.

    Example:
        >>> import daft
        >>> daft.range(2, 5).show()
        ╭───────╮
        │ id    │
        │ ---   │
        │ Int64 │
        ╞═══════╡
        │ 2     │
        ├╌╌╌╌╌╌╌┤
        │ 3     │
        ├╌╌╌╌╌╌╌┤
        │ 4     │
        ╰───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """

@PublicAPI
@overload
def _range(start: int, end: int, step: int) -> DataFrame:
    ...
    """Creates a DataFrame with a range of values.

    The range starts at `start` and ends at `end` (exclusive) with a step size of `step`.

    Example:
        >>> import daft
        >>> daft.range(2, 10, 2).show()
        ╭───────╮
        │ id    │
        │ ---   │
        │ Int64 │
        ╞═══════╡
        │ 2     │
        ├╌╌╌╌╌╌╌┤
        │ 4     │
        ├╌╌╌╌╌╌╌┤
        │ 6     │
        ├╌╌╌╌╌╌╌┤
        │ 8     │
        ╰───────╯
        <BLANKLINE>
        (Showing first 4 of 4 rows)
    """

@PublicAPI
@overload
def _range(start: int, end: int, step: int, partitions: int) -> DataFrame:
    """Creates a DataFrame with a range of values.

    The range starts at `start` and ends at `end` (exclusive) with a step size of `step`.
    The range is partitioned into `partitions` partitions.

    Example:
        >>> import daft
        >>> df = daft.range(2, 10, step=2, partitions=2)
        >>> df.num_partitions()
        2
        >>> df.show()
        ╭───────╮
        │ id    │
        │ ---   │
        │ Int64 │
        ╞═══════╡
        │ 2     │
        ├╌╌╌╌╌╌╌┤
        │ 4     │
        ├╌╌╌╌╌╌╌┤
        │ 6     │
        ├╌╌╌╌╌╌╌┤
        │ 8     │
        ╰───────╯
        <BLANKLINE>
        (Showing first 4 of 4 rows)
    """
    ...

@PublicAPI
def _range(start: int, end: Optional[int]=None, step: int = 1, partitions: int = 1) -> DataFrame:

    if end is None:
        end = start
        start = 0
    else:
        start = start
    scan_op = RangeScanOperator(start, end, step, partitions)
    handle = ScanOperatorHandle.from_python_scan_operator(scan_op)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)

    return DataFrame(builder)


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
