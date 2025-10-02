"""Partitioning Functions."""

from __future__ import annotations

from daft.expressions import Expression


def partition_days(expr: Expression) -> Expression:
    """Partitioning Transform that returns the number of days since epoch (1970-01-01).

    Unlike other temporal partitioning expressions, this expression is date type instead of int. This is to conform to the behavior of other implementations of Iceberg partition transforms.

    Returns:
        Date Expression
    """
    return Expression._from_pyexpr(expr._expr.partitioning_days())


def partition_hours(expr: Expression) -> Expression:
    """Partitioning Transform that returns the number of hours since epoch (1970-01-01).

    Returns:
        Expression: Int32 Expression in hours
    """
    return Expression._from_pyexpr(expr._expr.partitioning_hours())


def partition_months(expr: Expression) -> Expression:
    """Partitioning Transform that returns the number of months since epoch (1970-01-01).

    Returns:
        Expression: Int32 Expression in months
    """
    return Expression._from_pyexpr(expr._expr.partitioning_months())


def partition_years(expr: Expression) -> Expression:
    """Partitioning Transform that returns the number of years since epoch (1970-01-01).

    Returns:
        Expression: Int32 Expression in years
    """
    return Expression._from_pyexpr(expr._expr.partitioning_years())


def partition_iceberg_bucket(expr: Expression, n: int) -> Expression:
    """Partitioning Transform that returns the Hash Bucket following the Iceberg Specification of murmur3_32_x86.

    See <https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements> for more details.

    Args:
        expr: the expression to bucket
        n: Number of buckets

    Returns:
        Expression: Int32 Expression with the Hash Bucket
    """
    return Expression._from_pyexpr(expr._expr.partitioning_iceberg_bucket(n))


def partition_iceberg_truncate(expr: Expression, w: int) -> Expression:
    """Partitioning Transform that truncates the input to a standard width `w` following the Iceberg Specification.

    See <https://iceberg.apache.org/spec/#truncate-transform-details> for more details.

    Args:
        expr: the expression to truncate
        w: width of the truncation

    Returns:
        Expression: Expression of the Same Type of the input
    """
    return Expression._from_pyexpr(expr._expr.partitioning_iceberg_truncate(w))
