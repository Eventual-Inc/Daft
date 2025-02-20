from __future__ import annotations

from typing import Any

from daft.daft import WindowBoundary as _WindowBoundary
from daft.daft import WindowFrame as _WindowFrame
from daft.daft import WindowFrameType as _WindowFrameType
from daft.daft import WindowSpec as _WindowSpec
from daft.expressions import col


class Window:
    """Describes how to partition data and in what order to apply the window function.

    This class provides a way to specify window definitions for window functions.
    Window functions operate on a group of rows (called a window frame) and return
    a result for each row based on the values in its window frame.
    """

    # Class-level constants for frame boundaries
    unbounded_preceding = _WindowBoundary.UnboundedPreceding()
    unbounded_following = _WindowBoundary.UnboundedFollowing()
    current_row = _WindowBoundary.CurrentRow()

    def __init__(self):
        self._spec = _WindowSpec.new()

    @classmethod
    def partition_by(cls, *cols: str | list[str]) -> Window:
        """Partitions the dataset by one or more columns.

        Args:
            cols: Columns on which to partition data.

        Returns:
            Window: A window specification with the given partitioning.

        Raises:
            ValueError: If no partition columns are specified.
        """
        if not cols:
            raise ValueError("At least one partition column must be specified")

        # Flatten list arguments
        flat_cols = []
        for c in cols:
            if isinstance(c, list):
                flat_cols.extend(c)
            else:
                flat_cols.append(c)

        # Create new Window with updated spec
        window = cls()
        window._spec = window._spec.with_partition_by([col(c)._expr for c in flat_cols])
        return window

    @classmethod
    def order_by(cls, *cols: str | list[str], ascending: bool | list[bool] = True) -> Window:
        """Orders rows within each partition by specified columns.

        Args:
            cols: Columns to determine ordering within the partition.
            ascending: Sort ascending (True) or descending (False).

        Returns:
            Window: A window specification with the given ordering.
        """
        # Flatten list arguments
        flat_cols = []
        for c in cols:
            if isinstance(c, list):
                flat_cols.extend(c)
            else:
                flat_cols.append(c)

        # Handle ascending parameter
        if isinstance(ascending, bool):
            asc_flags = [ascending] * len(flat_cols)
        else:
            if len(ascending) != len(flat_cols):
                raise ValueError("Length of ascending flags must match number of order by columns")
            asc_flags = ascending

        # Create new Window with updated spec
        window = cls()
        window._spec = window._spec.with_order_by([col(c)._expr for c in flat_cols], asc_flags)
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
        # Convert integer offsets to WindowBoundary
        if isinstance(start, int):
            start = _WindowBoundary.Preceding(-start) if start < 0 else _WindowBoundary.Following(start)
        if isinstance(end, int):
            end = _WindowBoundary.Preceding(-end) if end < 0 else _WindowBoundary.Following(end)

        frame = _WindowFrame(
            frame_type=_WindowFrameType.Rows(),
            start=start,
            end=end,
        )

        # Create new Window with updated spec
        new_window = Window()
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
