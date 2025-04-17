from __future__ import annotations

from typing import Any

from daft.daft import WindowBoundary as _WindowBoundary
from daft.daft import WindowFrame as _WindowFrame
from daft.daft import WindowFrameType as _WindowFrameType
from daft.daft import WindowSpec as _WindowSpec
from daft.utils import ManyColumnsInputType, column_inputs_to_expressions


class Window:
    """Describes how to partition data and in what order to apply the window function.

    This class provides a way to specify window definitions for window functions.
    Window functions operate on a group of rows (called a window frame) and return
    a result for each row based on the values in its window frame.

    Basic window aggregation with a single partition column:

    >>> from daft import Window, col
    >>> # Define a window partitioned by category
    >>> window_spec = Window().partition_by("category")
    >>> # Apply aggregation functions
    >>> df = df.select(
    ...     col("value").sum().over(window_spec).alias("category_total"),
    ...     col("value").mean().over(window_spec).alias("category_avg"),
    ... )

    Partitioning by multiple columns:

    >>> # Define a window partitioned by both department and category
    >>> window_spec = Window().partition_by(["department", "category"])
    >>> df = df.select(col("sales").sum().over(window_spec).alias("dept_category_total"))

    Using window aggregations in expressions:

    >>> window_spec = Window().partition_by("category")
    >>> df = df.select((col("value") / col("value").sum().over(window_spec)).alias("pct_of_category"))
    """

    # Class-level constants for frame boundaries
    unbounded_preceding = _WindowBoundary.unbounded_preceding()
    unbounded_following = _WindowBoundary.unbounded_following()
    current_row = _WindowBoundary.offset(0)

    def __init__(self):
        self._spec = _WindowSpec.new()

    def partition_by(self, *cols: ManyColumnsInputType) -> Window:
        """Partitions the dataset by one or more columns or expressions.

        Args:
            *cols: Columns or expressions on which to partition data.
                   Can be column names as strings, Expression objects, or iterables of these.

        Returns:
            Window: A window specification with the given partitioning.

        Raises:
            ValueError: If no partition columns are specified.
        """
        if not cols:
            raise ValueError("At least one partition column must be specified")

        expressions = []
        for c in cols:
            expressions.extend(column_inputs_to_expressions(c))

        if not expressions:
            raise ValueError("At least one partition column must be specified")

        window = self
        window._spec = self._spec.with_partition_by([expr._expr for expr in expressions])
        return window

    def order_by(self, *cols: ManyColumnsInputType, desc: bool | list[bool] = False) -> Window:
        """Orders rows within each partition by specified columns or expressions.

        Args:
            *cols: Columns or expressions to determine ordering within the partition.
                   Can be column names as strings, Expression objects, or iterables of these.
            desc: Sort descending (True) or ascending (False). Can be a single boolean value applied to all columns,
                 or a list of boolean values corresponding to each column. Default is False (ascending).

        Returns:
            Window: A window specification with the given ordering.
        """
        expressions = []
        for c in cols:
            expressions.extend(column_inputs_to_expressions(c))

        if isinstance(desc, bool):
            desc_flags = [desc] * len(expressions)
        else:
            if len(desc) != len(expressions):
                raise ValueError("Length of descending flags must match number of order by columns")
            desc_flags = desc

        window = self
        window._spec = self._spec.with_order_by([expr._expr for expr in expressions], desc_flags)
        return window

    def rows_between(
        self,
        start: int | Any = unbounded_preceding,
        end: int | Any = unbounded_following,
        min_periods: int = 1,
    ) -> Window:
        """Restricts each window to a row-based frame between start and end boundaries.

        Args:
            start: Boundary definitions (unbounded_preceding, unbounded_following, current_row, or integer offsets)
            end: Boundary definitions
            min_periods: Minimum rows required to compute a result (default = 1)

        Returns:
            Window: A window specification with the given frame bounds.
        """
        if isinstance(start, int):
            start = _WindowBoundary.offset(start)
        if isinstance(end, int):
            end = _WindowBoundary.offset(end)

        frame = _WindowFrame(
            frame_type=_WindowFrameType.Rows,
            start=start,
            end=end,
        )

        new_window = self
        new_window._spec = self._spec.with_frame(frame).with_min_periods(min_periods)
        return new_window

    def range_between(
        self,
        start: int | Any = unbounded_preceding,
        end: int | Any = unbounded_following,
        min_periods: int = 1,
    ) -> Window:
        """Restricts each window to a range-based frame between start and end boundaries.

        Args:
            start: Boundary definitions (unbounded_preceding, unbounded_following, current_row, or numeric/time offsets)
            end: Boundary definitions
            min_periods: Minimum rows required to compute a result (default = 1)

        Returns:
            Window: A window specification with the given frame bounds.
        """
        raise NotImplementedError("Window.range_between is not implemented yet")
