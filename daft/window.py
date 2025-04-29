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

    Examples:
        >>> from daft import Window, col
        >>>
        >>> # Basic window aggregation with a single partition column:
        >>> window_spec = Window().partition_by("category")
        >>> df = df.select(
        ...     col("value").sum().over(window_spec).alias("category_total"),
        ...     col("value").mean().over(window_spec).alias("category_avg"),
        ... )
        >>>
        >>> # Partitioning by multiple columns:
        >>> window_spec = Window().partition_by(["department", "category"])
        >>> df = df.select(col("sales").sum().over(window_spec).alias("dept_category_total"))
        >>>
        >>> # Using window aggregations in expressions:
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

        Examples:
            >>> from daft import Window, col
            >>> # Partition by a single column 'category'
            >>> window_spec = Window().partition_by("category")
            >>> # Partition by multiple columns 'department' and 'region'
            >>> window_spec_multi = Window().partition_by("department", "region")
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

        Examples:
            >>> from daft import Window, col
            >>> # Order by 'date' ascending (default)
            >>> window_spec = Window().partition_by("category").order_by("date")
            >>> # Order by 'sales' descending
            >>> window_spec_desc = Window().partition_by("category").order_by("sales", desc=True)
            >>> # Order by 'date' ascending and 'sales' descending
            >>> window_spec_multi = Window().partition_by("category").order_by("date", "sales", desc=[False, True])
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

        This defines a sliding window based on row offsets relative to the current row.

        Args:
            start: Boundary definitions for the start of the window. Can be:

                *   ``Window.unbounded_preceding``: Include all rows before the current row.
                *   ``Window.current_row``: Start at the current row.
                *   Integer value (e.g., ``-3``): A negative integer indicates the number of rows *preceding* the current row.
                *   Integer value (e.g., ``1``): A positive integer indicates the number of rows *following* the current row.
            end: Boundary definitions for the end of the window. Can be:

                *   ``Window.unbounded_following``: Include all rows after the current row.
                *   ``Window.current_row``: End at the current row.
                *   Integer value (e.g., ``1``): A positive integer indicates the number of rows *following* the current row.
                *   Integer value (e.g., ``-1``): A negative integer indicates the number of rows *preceding* the current row.
            min_periods: Minimum number of rows required in the window frame to compute a result (default = 1).
                If fewer rows exist in the frame, the function returns NULL.

        Returns:
            Window: A window specification with the given row-based frame bounds.

        Examples:
            >>> from daft import Window
            >>> # Frame includes the current row and the 2 preceding rows
            >>> window_spec = Window().partition_by("cat").order_by("val").rows_between(-2, Window.current_row)
            >>> # Frame includes all rows from the beginning of the partition up to the current row
            >>> cum_window = (
            ...     Window()
            ...     .partition_by("cat")
            ...     .order_by("val")
            ...     .rows_between(Window.unbounded_preceding, Window.current_row)
            ... )
            >>> # Frame includes the preceding row, current row, and following row
            >>> sliding_window = Window().partition_by("cat").order_by("val").rows_between(-1, 1)
            >>> # Frame includes the current row and the 3 following rows, requiring at least 2 rows
            >>> lookahead_window = (
            ...     Window().partition_by("cat").order_by("val").rows_between(Window.current_row, 3, min_periods=2)
            ... )
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

        This defines a window frame based on a range of values relative to the current row's
        value in the ordering column. Requires exactly one `order_by` column, which must be
        numeric or temporal type.

        Args:
            start: Boundary definition for the start of the window's range. Can be:

                *   ``Window.unbounded_preceding``: Include all rows with order value <= current row's order value + start offset.
                *   ``Window.current_row``: Start range at the current row's order value.
                *   Offset value (e.g., ``-10``, ``datetime.timedelta(days=-1)``): The start of the range is defined as
                    `current_row_order_value + start`. The type of the offset must match the order-by column type.
                    Negative values indicate offsets *before* the current row's value, when in ascending order.
            end: Boundary definition for the end of the window's range. Syntax is similar to `start`.
                Positive values indicate offsets *after* the current row's value.
            min_periods: Minimum number of rows required in the window frame to compute a result (default = 1).
                If fewer rows exist in the frame, the function returns NULL.

        Returns:
            Window: A window specification with the given range-based frame bounds.

        Raises:
            NotImplementedError: This feature is not yet implemented.

        Examples:
            >>> from daft import Window, col
            >>> import datetime
            >>> # Assume df has columns 'sensor_id', 'timestamp', 'reading'
            >>> # Frame includes rows within 10 units *before* the current row's reading
            >>> val_window = Window().partition_by("sensor_id").order_by("reading").range_between(-10, Window.current_row)
            >>> # Frame includes rows from 1 day before to 1 day after the current row's timestamp
            >>> time_window = (
            ...     Window()
            ...     .partition_by("sensor_id")
            ...     .order_by("timestamp")
            ...     .range_between(datetime.timedelta(days=-1), datetime.timedelta(days=1))
            ... )
        """
        raise NotImplementedError("Window.range_between is not implemented yet")
