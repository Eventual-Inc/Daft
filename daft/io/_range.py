from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, overload

if TYPE_CHECKING:
    from collections.abc import Iterator


from daft import DataType
from daft.api_annotations import PublicAPI
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.io.pushdowns import Pushdowns


@overload
def _range(end: int) -> DataFrame: ...


@overload
def _range(start: int, end: int) -> DataFrame: ...


@overload
def _range(start: int, end: int, step: int) -> DataFrame: ...


@overload
def _range(start: int, end: int, step: int, partitions: int) -> DataFrame: ...


# TODO: consider using `from_range` and `Series.from_range` instead.
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
    return RangeSource(start, end, step, partitions).read()


class RangeSource(DataSource):
    """RangeSource produces a DataFrame from a range with a given step size."""

    _start: int
    _end: int
    _step: int
    _partitions: int
    _schema = Schema.from_pydict({"id": DataType.int64()})

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

    @property
    def name(self) -> str:
        return "RangeSource"

    @property
    def schema(self) -> Schema:
        return self._schema

    def get_tasks(self, pushdowns: Pushdowns) -> Iterator[RangeSourceTask]:
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

    @property
    def schema(self) -> Schema:
        return RangeSource._schema

    def get_micro_partitions(self) -> Iterator[MicroPartition]:
        yield MicroPartition.from_pydict({"id": list(range(self._start, self._end, self._step))})
