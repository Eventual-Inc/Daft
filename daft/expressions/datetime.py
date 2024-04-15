from __future__ import annotations

from daft import Expression
from daft.expressions.expressions import ExpressionNamespace


class ExpressionDatetimeNamespace(ExpressionNamespace):
    def date(self) -> Expression:
        """Retrieves the date for a datetime column

        Example:
            >>> col("x").dt.date()

        Returns:
            Expression: a Date expression
        """
        return Expression._from_pyexpr(self._expr.dt_date())

    def day(self) -> Expression:
        """Retrieves the day for a datetime column

        Example:
            >>> col("x").dt.day()

        Returns:
            Expression: a UInt32 expression with just the day extracted from a datetime column
        """
        return Expression._from_pyexpr(self._expr.dt_day())

    def hour(self) -> Expression:
        """Retrieves the day for a datetime column

        Example:
            >>> col("x").dt.day()

        Returns:
            Expression: a UInt32 expression with just the day extracted from a datetime column
        """
        return Expression._from_pyexpr(self._expr.dt_hour())

    def month(self) -> Expression:
        """Retrieves the month for a datetime column

        Example:
            >>> col("x").dt.month()

        Returns:
            Expression: a UInt32 expression with just the month extracted from a datetime column
        """
        return Expression._from_pyexpr(self._expr.dt_month())

    def year(self) -> Expression:
        """Retrieves the year for a datetime column

        Example:
            >>> col("x").dt.year()

        Returns:
            Expression: a UInt32 expression with just the year extracted from a datetime column
        """
        return Expression._from_pyexpr(self._expr.dt_year())

    def day_of_week(self) -> Expression:
        """Retrieves the day of the week for a datetime column, starting at 0 for Monday and ending at 6 for Sunday

        Example:
            >>> col("x").dt.day_of_week()

        Returns:
            Expression: a UInt32 expression with just the day_of_week extracted from a datetime column
        """
        return Expression._from_pyexpr(self._expr.dt_day_of_week())
