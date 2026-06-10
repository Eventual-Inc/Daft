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


def extract_minute_uuid7(expr: Expression) -> Expression:
    """Partitioning Transform that extracts the number of minutes since epoch (1970-01-01) from a UUIDv7.

    A UUIDv7 embeds a 48-bit Unix-millisecond timestamp in its first 6 bytes. The input must be a
    Uuid or a FixedSizeBinary of 16 bytes (128 bits).

    Args:
        expr: a Uuid or FixedSizeBinary(16) expression of UUIDv7 values

    Returns:
        Expression: Int64 Expression with the number of minutes since epoch
    """
    return Expression._call_builtin_scalar_fn("extract_minute_uuid7", expr)


def extract_hour_uuid7(expr: Expression) -> Expression:
    """Partitioning Transform that extracts the number of hours since epoch (1970-01-01) from a UUIDv7.

    A UUIDv7 embeds a 48-bit Unix-millisecond timestamp in its first 6 bytes. The input must be a
    Uuid or a FixedSizeBinary of 16 bytes (128 bits).

    Args:
        expr: a Uuid or FixedSizeBinary(16) expression of UUIDv7 values

    Returns:
        Expression: Int64 Expression with the number of hours since epoch
    """
    return Expression._call_builtin_scalar_fn("extract_hour_uuid7", expr)


def extract_day_uuid7(expr: Expression) -> Expression:
    """Partitioning Transform that extracts the number of days since epoch (1970-01-01) from a UUIDv7.

    A UUIDv7 embeds a 48-bit Unix-millisecond timestamp in its first 6 bytes. The input must be a
    Uuid or a FixedSizeBinary of 16 bytes (128 bits).

    Args:
        expr: a Uuid or FixedSizeBinary(16) expression of UUIDv7 values

    Returns:
        Expression: Int64 Expression with the number of days since epoch
    """
    return Expression._call_builtin_scalar_fn("extract_day_uuid7", expr)


def extract_month_uuid7(expr: Expression) -> Expression:
    """Partitioning Transform that extracts the number of calendar months since 1970-01 from a UUIDv7.

    The result is `(year - 1970) * 12 + (month - 1)`, matching `partition_months`. A UUIDv7 embeds a
    48-bit Unix-millisecond timestamp in its first 6 bytes. The input must be a Uuid or a
    FixedSizeBinary of 16 bytes (128 bits).

    Args:
        expr: a Uuid or FixedSizeBinary(16) expression of UUIDv7 values

    Returns:
        Expression: Int64 Expression with the number of months since 1970-01
    """
    return Expression._call_builtin_scalar_fn("extract_month_uuid7", expr)
