from typing import List, Union


class WindowBoundary:
    """Represents window frame boundaries."""

    def __init__(self, name: str):
        self.name = name

    def __repr__(self) -> str:
        return f"WindowBoundary({self.name})"


class Window:
    """Describes how to partition data and in what order to apply the window function.

    This class provides a way to specify window definitions for window functions.
    Window functions operate on a group of rows (called a window frame) and return
    a result for each row based on the values in its window frame.
    """

    # Class-level constants for frame boundaries
    unbounded_preceding = WindowBoundary("UNBOUNDED PRECEDING")
    unbounded_following = WindowBoundary("UNBOUNDED FOLLOWING")
    current_row = WindowBoundary("CURRENT ROW")

    def __init__(self):
        self.partition_by = None
        self.order_by = None
        self.frame_start = self.unbounded_preceding
        self.frame_end = self.unbounded_following

    def __repr__(self) -> str:
        return f"Window(partition_by={self.partition_by}, order_by={self.order_by}, frame_start={self.frame_start}, frame_end={self.frame_end})"

    @staticmethod
    def partition_by(*cols: Union[str, List[str]]) -> "Window":
        """Partitions the dataset by one or more columns.

        Args:
            cols: Columns on which to partition data.

        Returns:
            Window: A window specification with the given partitioning.

        Raises:
            ValueError: If no partition columns are specified.
        """
        raise NotImplementedError

    def order_by(self, *cols: Union[str, List[str]], ascending: Union[bool, List[bool]] = True) -> "Window":
        """Orders rows within each partition by specified columns.

        Args:
            cols: Columns to determine ordering within the partition.
            ascending: Sort ascending (True) or descending (False).

        Returns:
            Window: A window specification with the given ordering.
        """
        raise NotImplementedError

    def rows_between(
        self,
        start: Union[int, WindowBoundary] = unbounded_preceding,
        end: Union[int, WindowBoundary] = unbounded_following,
        min_periods: int = 1,
    ) -> "Window":
        """Restricts each window to a row-based frame between start and end boundaries.

        Args:
            start: Boundary definitions (unbounded_preceding, unbounded_following, current_row, or integer offsets)
            end: Boundary definitions
            min_periods: Minimum rows required to compute a result (default = 1)

        Returns:
            Window: A window specification with the given frame bounds.
        """
        raise NotImplementedError

    def range_between(
        self,
        start: Union[int, WindowBoundary] = unbounded_preceding,
        end: Union[int, WindowBoundary] = unbounded_following,
        min_periods: int = 1,
    ) -> "Window":
        """Restricts each window to a range-based frame between start and end boundaries.

        Args:
            start: Boundary definitions (unbounded_preceding, unbounded_following, current_row, or numeric/time offsets)
            end: Boundary definitions
            min_periods: Minimum rows required to compute a result (default = 1)

        Returns:
            Window: A window specification with the given frame bounds.
        """
        raise NotImplementedError
