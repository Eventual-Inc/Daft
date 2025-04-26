from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Iterator, overload

if TYPE_CHECKING:
    from collections.abc import Iterator


from daft import DataType
from daft.api_annotations import PublicAPI
from daft.dataframe import DataFrame, dataframe
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch.recordbatch import RecordBatch
from daft.schema import schema

if TYPE_CHECKING:
    from daft.io.pushdowns import Pushdowns
    from daft.schema import Schema


@overload
def _range(end: int) -> DataFrame: ...


@overload
def _range(start: int, end: int) -> DataFrame: ...


@overload
def _range(start: int, end: int, step: int) -> DataFrame: ...


@overload
def _range(start: int, end: int, step: int, partitions: int) -> DataFrame: ...


@PublicAPI  # type: ignore
def _range(start: int, end: int | None = None, step: int = 1, partitions: int = 1) -> DataFrame:
    """Creates a DataFrame with a range of values.

    Args:
        start (int): The start of the range.
        end (int, optional): The end of the range. If not provided, the start is 0 and the end is `start`.
        step (int, optional): The step size of the range. Defaults to 1.
        partitions (int, optional): The number of partitions to split the range into. Defaults to 1.

    Examples:
        The range starts at 0 and ends at `end` (exclusive) with a step size of 1.

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

        The range starts at `start` and ends at `end` (exclusive) with a step size of 1.

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

        The range starts at `start` and ends at `end` (exclusive) with a step size of `step`.

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

        The range starts at `start` and ends at `end` (exclusive) with a step size of `step`.
        The range is partitioned into `partitions` partitions.

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
    if end is None:
        end = start
        start = 0
    else:
        start = start
    source = RangeSource(start, end, step, partitions)
    return dataframe(source)


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


# class RangeScanOperator(GeneratorScanOperator):
#     def __init__(self, start: int, end: int, step: int = 1, partitions: int = 1) -> None:
#         schema = Schema._from_field_name_and_types([("id", DataType.int64())])

#         super().__init__(schema=schema, generators=_range_generators(start, end, step, partitions))


class RangeSource(DataSource):
    """RangeSource produces a DataFrame from a range with a given step size."""

    _start: int
    _end: int
    _step: int
    _partitions: int

    def __init__(self, start: int, end: int, step: int = 1, partitions: int = 1) -> None:
        """Create a RangeSource instance.

        Args:
            start (int): The start of the range.
            end (int, optional): The end of the range. If not provided, the start is 0 and the end is `start`.
            step (int, optional): The step size of the range. Defaults to 1.
            partitions (int, optional): The number of partitions to split the range into. Defaults to 1.
        """
        self._start = start
        self._end = end
        self._step = step
        self._partitions = partitions

    def name(self) -> str:
        return "RangeSource"

    def schema(self) -> Schema:
        return schema({"id": DataType.int64()})

    def get_tasks(self, pushdowns: Pushdowns) -> Iterator[DataSourceTask]:
        step = self._step
        size = (self._end - self._start) // self._partitions
        curr_s = self._start
        curr_e = self._start + size
        while curr_e <= self._end:
            yield RangeSourceTask(curr_s, curr_e, step)
            # update to the next chunk
            curr_s = curr_e + step
            curr_e = curr_s + size


@dataclass
class RangeSourceTask(DataSourceTask):
    _start: int
    _end: int
    _step: int

    def schema(self) -> Schema:
        return schema({"id": DataType.int64()})

    def get_batches(self) -> Iterator[RecordBatch]:
        import pyarrow as pa

        values = list(range(self._start, self._end, self._step))
        table = pa.Table.from_arrays([pa.array(values, type=pa.int64())], names=["id"])
        yield RecordBatch.from_arrow(table)
