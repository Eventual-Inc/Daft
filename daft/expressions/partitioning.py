from __future__ import annotations

from daft import Expression
from daft.expressions.expressions import ExpressionNamespace


class ExpressionPartitioningNamespace(ExpressionNamespace):
    def days(self) -> Expression:
        """Partitioning Transform that returns the number of days since epoch (1970-01-01)

        Returns:
            Expression: Date Type Expression
        """
        return Expression._from_pyexpr(self._expr.partitioning_days())

    def hours(self) -> Expression:
        """Partitioning Transform that returns the number of hours since epoch (1970-01-01)

        Returns:
            Expression: Int32 Expression in hours
        """
        return Expression._from_pyexpr(self._expr.partitioning_hours())

    def months(self) -> Expression:
        """Partitioning Transform that returns the number of months since epoch (1970-01-01)

        Returns:
            Expression: Int32 Expression in months
        """

        return Expression._from_pyexpr(self._expr.partitioning_months())

    def years(self) -> Expression:
        """Partitioning Transform that returns the number of years since epoch (1970-01-01)

        Returns:
            Expression: Int32 Expression in years
        """

        return Expression._from_pyexpr(self._expr.partitioning_years())

    def iceberg_bucket(self, n: int) -> Expression:
        """Partitioning Transform that returns the Hash Bucket following the Iceberg Specification of murmur3_32_x86
        https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements

        Args:
            n (int): Number of buckets

        Returns:
            Expression: Int32 Expression with the Hash Bucket
        """
        return Expression._from_pyexpr(self._expr.partitioning_iceberg_bucket(n))

    def iceberg_truncate(self, w: int) -> Expression:
        """Partitioning Transform that truncates the input to a standard width `w` following the Iceberg Specification.
        https://iceberg.apache.org/spec/#truncate-transform-details

        Args:
            w (int): width of the truncation

        Returns:
            Expression: Expression of the Same Type of the input
        """
        return Expression._from_pyexpr(self._expr.partitioning_iceberg_truncate(w))
