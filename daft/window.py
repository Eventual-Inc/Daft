from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable

from daft.daft import WindowBoundary as _WindowBoundary
from daft.daft import WindowFrame as _WindowFrame
from daft.daft import WindowFrameType as _WindowFrameType
from daft.daft import WindowSpec as _WindowSpec
from daft.expressions import Expression, col

# Import column input types from DataFrame for type checking
if TYPE_CHECKING:
    from daft.dataframe.dataframe import ColumnInputType, ManyColumnsInputType


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

    @staticmethod
    def _is_column_input(x: Any) -> bool:
        return isinstance(x, str) or isinstance(x, Expression)

    @staticmethod
    def _column_inputs_to_expressions(columns: ManyColumnsInputType) -> list[Expression]:
        """Inputs can be passed in as individual arguments or an iterable.

        In addition, they may be strings or Expressions.
        This method normalizes the inputs to a list of Expressions.
        """
        column_iter: Iterable[ColumnInputType] = [columns] if Window._is_column_input(columns) else columns  # type: ignore
        return [col(c) if isinstance(c, str) else c for c in column_iter]

    @classmethod
    def partition_by(cls, *cols: ManyColumnsInputType) -> Window:
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

        # Handle column inputs similar to DataFrame methods
        expressions = []
        for c in cols:
            expressions.extend(cls._column_inputs_to_expressions(c))

        if not expressions:
            raise ValueError("At least one partition column must be specified")

        # Create new Window with updated spec
        window = cls()
        window._spec = window._spec.with_partition_by([expr._expr for expr in expressions])
        return window

    @classmethod
    def order_by(cls, *cols: ManyColumnsInputType, ascending: bool | list[bool] = True) -> Window:
        """Orders rows within each partition by specified columns or expressions.

        Args:
            *cols: Columns or expressions to determine ordering within the partition.
                   Can be column names as strings, Expression objects, or iterables of these.
            ascending: Sort ascending (True) or descending (False).

        Returns:
            Window: A window specification with the given ordering.
        """
        # Handle column inputs similar to DataFrame methods
        expressions = []
        for c in cols:
            expressions.extend(cls._column_inputs_to_expressions(c))

        # Handle ascending parameter
        if isinstance(ascending, bool):
            asc_flags = [ascending] * len(expressions)
        else:
            if len(ascending) != len(expressions):
                raise ValueError("Length of ascending flags must match number of order by columns")
            asc_flags = ascending

        # Create new Window with updated spec
        window = cls()
        window._spec = window._spec.with_order_by([expr._expr for expr in expressions], asc_flags)
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
            start = _WindowBoundary.Offset(start)
        if isinstance(end, int):
            end = _WindowBoundary.Offset(end)

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
