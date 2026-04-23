"""Date and Time Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

from daft.expressions import Expression

if TYPE_CHECKING:
    from daft.datatype import TimeUnit


def date(expr: Expression) -> Expression:
    """Retrieves the date for a datetime column.

    Returns:
        Expression: a Date expression

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import date
        >>> df = daft.from_pydict(
        ...     {
        ...         "x": [
        ...             datetime.datetime(2021, 1, 1, 5, 1, 1),
        ...             datetime.datetime(2021, 1, 2, 6, 1, 59),
        ...             datetime.datetime(2021, 1, 3, 7, 2, 0),
        ...         ],
        ...     }
        ... )
        >>> df = df.with_column("date", date(df["x"]))
        >>> df.show()
        ╭─────────────────────┬────────────╮
        │ x                   ┆ date       │
        │ ---                 ┆ ---        │
        │ Timestamp[us]       ┆ Date       │
        ╞═════════════════════╪════════════╡
        │ 2021-01-01 05:01:01 ┆ 2021-01-01 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2021-01-02 06:01:59 ┆ 2021-01-02 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2021-01-03 07:02:00 ┆ 2021-01-03 │
        ╰─────────────────────┴────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("date", expr)


def day(expr: Expression) -> Expression:
    """Retrieves the day for a datetime column.

    Returns:
        Expression: a UInt32 expression with just the day extracted from a datetime column

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import day
        >>> df = daft.from_pydict(
        ...     {
        ...         "x": [
        ...             datetime.datetime(2021, 1, 1, 5, 1, 1),
        ...             datetime.datetime(2021, 1, 2, 6, 1, 59),
        ...             datetime.datetime(2021, 1, 3, 7, 2, 0),
        ...         ],
        ...     }
        ... )
        >>> df = df.with_column("day", day(df["x"]))
        >>> df.show()
        ╭─────────────────────┬────────╮
        │ x                   ┆ day    │
        │ ---                 ┆ ---    │
        │ Timestamp[us]       ┆ UInt32 │
        ╞═════════════════════╪════════╡
        │ 2021-01-01 05:01:01 ┆ 1      │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ 2021-01-02 06:01:59 ┆ 2      │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ 2021-01-03 07:02:00 ┆ 3      │
        ╰─────────────────────┴────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("day", expr)


def hour(expr: Expression) -> Expression:
    """Retrieves the hour for a datetime column.

    Returns:
        Expression: a UInt32 expression with just the hour extracted from a datetime column

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import hour
        >>> df = daft.from_pydict(
        ...     {
        ...         "x": [
        ...             datetime.datetime(2021, 1, 1, 5, 1, 1),
        ...             datetime.datetime(2021, 1, 2, 6, 1, 59),
        ...             datetime.datetime(2021, 1, 3, 7, 2, 0),
        ...         ],
        ...     }
        ... )
        >>> df = df.with_column("hour", hour(df["x"]))
        >>> df.show()
        ╭─────────────────────┬────────╮
        │ x                   ┆ hour   │
        │ ---                 ┆ ---    │
        │ Timestamp[us]       ┆ UInt32 │
        ╞═════════════════════╪════════╡
        │ 2021-01-01 05:01:01 ┆ 5      │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ 2021-01-02 06:01:59 ┆ 6      │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ 2021-01-03 07:02:00 ┆ 7      │
        ╰─────────────────────┴────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("hour", expr)


def minute(expr: Expression) -> Expression:
    """Retrieves the minute for a datetime column.

    Returns:
        Expression: a UInt32 expression with just the minute extracted from a datetime column

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import minute
        >>> df = daft.from_pydict(
        ...     {
        ...         "x": [
        ...             datetime.datetime(2021, 1, 1, 5, 1, 1),
        ...             datetime.datetime(2021, 1, 2, 6, 1, 59),
        ...             datetime.datetime(2021, 1, 3, 7, 2, 0),
        ...         ],
        ...     }
        ... )
        >>> df = df.with_column("minute", minute(df["x"]))
        >>> df.show()
        ╭─────────────────────┬────────╮
        │ x                   ┆ minute │
        │ ---                 ┆ ---    │
        │ Timestamp[us]       ┆ UInt32 │
        ╞═════════════════════╪════════╡
        │ 2021-01-01 05:01:01 ┆ 1      │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ 2021-01-02 06:01:59 ┆ 1      │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ 2021-01-03 07:02:00 ┆ 2      │
        ╰─────────────────────┴────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("minute", expr)


def second(expr: Expression) -> Expression:
    """Retrieves the second for a datetime column.

    Returns:
        Expression: a UInt32 expression with just the second extracted from a datetime column

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import second
        >>> df = daft.from_pydict(
        ...     {
        ...         "x": [
        ...             datetime.datetime(2021, 1, 1, 0, 1, 1),
        ...             datetime.datetime(2021, 1, 1, 0, 1, 59),
        ...             datetime.datetime(2021, 1, 1, 0, 2, 0),
        ...         ],
        ...     }
        ... )
        >>> df = df.with_column("second", second(df["x"]))
        >>> df.show()
        ╭─────────────────────┬────────╮
        │ x                   ┆ second │
        │ ---                 ┆ ---    │
        │ Timestamp[us]       ┆ UInt32 │
        ╞═════════════════════╪════════╡
        │ 2021-01-01 00:01:01 ┆ 1      │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ 2021-01-01 00:01:59 ┆ 59     │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ 2021-01-01 00:02:00 ┆ 0      │
        ╰─────────────────────┴────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("second", expr)


def millisecond(expr: Expression) -> Expression:
    """Retrieves the millisecond for a datetime column.

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import millisecond
        >>> df = daft.from_pydict(
        ...     {
        ...         "datetime": [
        ...             datetime.datetime(1978, 1, 1, 1, 1, 1, 0),
        ...             datetime.datetime(2024, 10, 13, 5, 30, 14, 500_000),
        ...             datetime.datetime(2065, 1, 1, 10, 20, 30, 60_000),
        ...         ]
        ...     }
        ... )
        >>> df = df.select(millisecond(df["datetime"]))
        >>> df.show()
        ╭──────────╮
        │ datetime │
        │ ---      │
        │ UInt32   │
        ╞══════════╡
        │ 0        │
        ├╌╌╌╌╌╌╌╌╌╌┤
        │ 500      │
        ├╌╌╌╌╌╌╌╌╌╌┤
        │ 60       │
        ╰──────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    return Expression._call_builtin_scalar_fn("millisecond", expr)


def microsecond(expr: Expression) -> Expression:
    """Retrieves the microsecond for a datetime column.

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import microsecond
        >>> df = daft.from_pydict(
        ...     {
        ...         "datetime": [
        ...             datetime.datetime(1978, 1, 1, 1, 1, 1, 0),
        ...             datetime.datetime(2024, 10, 13, 5, 30, 14, 500_000),
        ...             datetime.datetime(2065, 1, 1, 10, 20, 30, 60_000),
        ...         ]
        ...     }
        ... )
        >>> df.select(microsecond(df["datetime"])).show()
        ╭──────────╮
        │ datetime │
        │ ---      │
        │ UInt32   │
        ╞══════════╡
        │ 0        │
        ├╌╌╌╌╌╌╌╌╌╌┤
        │ 500000   │
        ├╌╌╌╌╌╌╌╌╌╌┤
        │ 60000    │
        ╰──────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("microsecond", expr)


def nanosecond(expr: Expression) -> Expression:
    """Retrieves the nanosecond for a datetime column.

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import nanosecond
        >>> df = daft.from_pydict(
        ...     {
        ...         "datetime": [
        ...             datetime.datetime(1978, 1, 1, 1, 1, 1, 0),
        ...             datetime.datetime(2024, 10, 13, 5, 30, 14, 500_000),
        ...             datetime.datetime(2065, 1, 1, 10, 20, 30, 60_000),
        ...         ]
        ...     }
        ... )
        >>>
        >>> df.select(nanosecond(df["datetime"])).show()
        ╭───────────╮
        │ datetime  │
        │ ---       │
        │ UInt32    │
        ╞═══════════╡
        │ 0         │
        ├╌╌╌╌╌╌╌╌╌╌╌┤
        │ 500000000 │
        ├╌╌╌╌╌╌╌╌╌╌╌┤
        │ 60000000  │
        ╰───────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("nanosecond", expr)


def unix_date(expr: Expression) -> Expression:
    """Retrieves the number of days since 1970-01-01 00:00:00 UTC.

    Returns:
        Expression: a UInt64 expression

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import unix_date
        >>> df = daft.from_pydict(
        ...     {
        ...         "datetime": [
        ...             datetime.datetime(1978, 1, 1, 1, 1, 1, 0),
        ...             datetime.datetime(2024, 10, 13, 5, 30, 14, 500_000),
        ...             datetime.datetime(2065, 1, 1, 10, 20, 30, 60_000),
        ...         ]
        ...     }
        ... )
        >>>
        >>> df.select(unix_date(df["datetime"]).alias("unix_date")).show()
        ╭───────────╮
        │ unix_date │
        │ ---       │
        │ UInt64    │
        ╞═══════════╡
        │ 2922      │
        ├╌╌╌╌╌╌╌╌╌╌╌┤
        │ 20009     │
        ├╌╌╌╌╌╌╌╌╌╌╌┤
        │ 34699     │
        ╰───────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("unix_date", expr)


def time(expr: Expression) -> Expression:
    """Retrieves the time for a datetime column.

    Returns:
        Expression: a Time expression

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import time
        >>> df = daft.from_pydict(
        ...     {
        ...         "x": [
        ...             datetime.datetime(2021, 1, 1, 0, 1, 1),
        ...             datetime.datetime(2021, 1, 1, 12, 1, 59),
        ...             datetime.datetime(2021, 1, 1, 23, 59, 59),
        ...         ],
        ...     }
        ... )
        >>> df = df.with_column("time", time(df["x"]))
        >>> df.show()
        ╭─────────────────────┬──────────╮
        │ x                   ┆ time     │
        │ ---                 ┆ ---      │
        │ Timestamp[us]       ┆ Time[us] │
        ╞═════════════════════╪══════════╡
        │ 2021-01-01 00:01:01 ┆ 00:01:01 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
        │ 2021-01-01 12:01:59 ┆ 12:01:59 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
        │ 2021-01-01 23:59:59 ┆ 23:59:59 │
        ╰─────────────────────┴──────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("time", expr)


def month(expr: Expression) -> Expression:
    """Retrieves the month for a datetime column.

    Returns:
        Expression: a UInt32 expression with just the month extracted from a datetime column

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import month
        >>> df = daft.from_pydict(
        ...     {
        ...         "datetime": [
        ...             datetime.datetime(2024, 7, 3, 0, 0, 0),
        ...             datetime.datetime(2024, 6, 4, 0, 0, 0),
        ...             datetime.datetime(2024, 5, 5, 0, 0, 0),
        ...         ],
        ...     }
        ... )
        >>> df.with_column("month", month(df["datetime"])).collect()
        ╭─────────────────────┬────────╮
        │ datetime            ┆ month  │
        │ ---                 ┆ ---    │
        │ Timestamp[us]       ┆ UInt32 │
        ╞═════════════════════╪════════╡
        │ 2024-07-03 00:00:00 ┆ 7      │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ 2024-06-04 00:00:00 ┆ 6      │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ 2024-05-05 00:00:00 ┆ 5      │
        ╰─────────────────────┴────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("month", expr)


def quarter(expr: Expression) -> Expression:
    """Retrieves the quarter for a datetime column.

    Returns:
        Expression: a UInt32 expression with just the quarter extracted from a datetime column

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import quarter
        >>> df = daft.from_pydict(
        ...     {
        ...         "datetime": [
        ...             datetime.datetime(2024, 1, 1, 0, 0, 0),
        ...             datetime.datetime(2023, 7, 4, 0, 0, 0),
        ...             datetime.datetime(2022, 12, 5, 0, 0, 0),
        ...         ],
        ...     }
        ... )
        >>> df.with_column("quarter", quarter(df["datetime"])).collect()
        ╭─────────────────────┬─────────╮
        │ datetime            ┆ quarter │
        │ ---                 ┆ ---     │
        │ Timestamp[us]       ┆ UInt32  │
        ╞═════════════════════╪═════════╡
        │ 2024-01-01 00:00:00 ┆ 1       │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ 2023-07-04 00:00:00 ┆ 3       │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ 2022-12-05 00:00:00 ┆ 4       │
        ╰─────────────────────┴─────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("quarter", expr)


def year(expr: Expression) -> Expression:
    """Retrieves the year for a datetime column.

    Returns:
        Expression: a Int32 expression with just the year extracted from a datetime column

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import year
        >>> df = daft.from_pydict(
        ...     {
        ...         "datetime": [
        ...             datetime.datetime(2024, 7, 3, 0, 0, 0),
        ...             datetime.datetime(2023, 7, 4, 0, 0, 0),
        ...             datetime.datetime(2022, 7, 5, 0, 0, 0),
        ...         ],
        ...     }
        ... )
        >>> df.with_column("year", year(df["datetime"])).collect()
        ╭─────────────────────┬───────╮
        │ datetime            ┆ year  │
        │ ---                 ┆ ---   │
        │ Timestamp[us]       ┆ Int32 │
        ╞═════════════════════╪═══════╡
        │ 2024-07-03 00:00:00 ┆ 2024  │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 2023-07-04 00:00:00 ┆ 2023  │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 2022-07-05 00:00:00 ┆ 2022  │
        ╰─────────────────────┴───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("year", expr)


def day_of_week(expr: Expression) -> Expression:
    """Retrieves the day of the week for a datetime column, starting at 0 for Monday and ending at 6 for Sunday.

    Returns:
        Expression: a UInt32 expression with just the day_of_week extracted from a datetime column

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import day_of_week
        >>> df = daft.from_pydict(
        ...     {
        ...         "datetime": [
        ...             datetime.datetime(2024, 7, 3, 0, 0, 0),
        ...             datetime.datetime(2024, 7, 4, 0, 0, 0),
        ...             datetime.datetime(2024, 7, 5, 0, 0, 0),
        ...         ],
        ...     }
        ... )
        >>> df.with_column("day_of_week", day_of_week(df["datetime"])).collect()
        ╭─────────────────────┬─────────────╮
        │ datetime            ┆ day_of_week │
        │ ---                 ┆ ---         │
        │ Timestamp[us]       ┆ UInt32      │
        ╞═════════════════════╪═════════════╡
        │ 2024-07-03 00:00:00 ┆ 2           │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2024-07-04 00:00:00 ┆ 3           │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2024-07-05 00:00:00 ┆ 4           │
        ╰─────────────────────┴─────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("day_of_week", expr)


def day_of_month(expr: Expression) -> Expression:
    """Retrieves the day of the month for a datetime column.

    Returns:
        Expression: a UInt32 expression with just the day_of_month extracted from a datetime column

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import day_of_month
        >>> df = daft.from_pydict(
        ...     {
        ...         "datetime": [
        ...             datetime.datetime(2024, 1, 1, 0, 0, 0),
        ...             datetime.datetime(2024, 2, 1, 0, 0, 0),
        ...             datetime.datetime(2024, 12, 31, 0, 0, 0),
        ...             datetime.datetime(2023, 12, 31, 0, 0, 0),
        ...         ],
        ...     }
        ... )
        >>> df.with_column("day_of_month", day_of_month(df["datetime"])).collect()
        ╭─────────────────────┬──────────────╮
        │ datetime            ┆ day_of_month │
        │ ---                 ┆ ---          │
        │ Timestamp[us]       ┆ UInt32       │
        ╞═════════════════════╪══════════════╡
        │ 2024-01-01 00:00:00 ┆ 1            │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2024-02-01 00:00:00 ┆ 1            │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2024-12-31 00:00:00 ┆ 31           │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2023-12-31 00:00:00 ┆ 31           │
        ╰─────────────────────┴──────────────╯
        <BLANKLINE>
        (Showing first 4 of 4 rows)
    """
    return Expression._call_builtin_scalar_fn("day_of_month", expr)


def day_of_year(expr: Expression) -> Expression:
    """Retrieves the ordinal day for a datetime column. Starting at 1 for January 1st and ending at 365 or 366 for December 31st.

    Returns:
        Expression: a UInt32 expression with just the day_of_year extracted from a datetime column

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import day_of_year
        >>> df = daft.from_pydict(
        ...     {
        ...         "datetime": [
        ...             datetime.datetime(2024, 1, 1, 0, 0, 0),
        ...             datetime.datetime(2024, 2, 1, 0, 0, 0),
        ...             datetime.datetime(2024, 12, 31, 0, 0, 0),  # 2024 is a leap year
        ...             datetime.datetime(2023, 12, 31, 0, 0, 0),  # not leap year
        ...         ],
        ...     }
        ... )
        >>> df.with_column("day_of_year", day_of_year(df["datetime"])).collect()
        ╭─────────────────────┬─────────────╮
        │ datetime            ┆ day_of_year │
        │ ---                 ┆ ---         │
        │ Timestamp[us]       ┆ UInt32      │
        ╞═════════════════════╪═════════════╡
        │ 2024-01-01 00:00:00 ┆ 1           │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2024-02-01 00:00:00 ┆ 32          │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2024-12-31 00:00:00 ┆ 366         │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2023-12-31 00:00:00 ┆ 365         │
        ╰─────────────────────┴─────────────╯
        <BLANKLINE>
        (Showing first 4 of 4 rows)
    """
    return Expression._call_builtin_scalar_fn("day_of_year", expr)


def week_of_year(expr: Expression) -> Expression:
    """Retrieves the week of the year for a datetime column.

    Returns:
        Expression: a UInt32 expression with just the week_of_year extracted from a datetime column

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import week_of_year
        >>> df = daft.from_pydict(
        ...     {
        ...         "datetime": [
        ...             datetime.datetime(2024, 1, 1, 0, 0, 0),
        ...             datetime.datetime(2024, 2, 1, 0, 0, 0),
        ...             datetime.datetime(
        ...                 2024, 12, 31, 0, 0, 0
        ...             ),  # part of week 1 of 2025 according to ISO 8601 standard
        ...             datetime.datetime(2023, 12, 31, 0, 0, 0),
        ...         ],
        ...     }
        ... )
        >>> df.with_column("week_of_year", week_of_year(df["datetime"])).collect()
        ╭─────────────────────┬──────────────╮
        │ datetime            ┆ week_of_year │
        │ ---                 ┆ ---          │
        │ Timestamp[us]       ┆ UInt32       │
        ╞═════════════════════╪══════════════╡
        │ 2024-01-01 00:00:00 ┆ 1            │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2024-02-01 00:00:00 ┆ 5            │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2024-12-31 00:00:00 ┆ 1            │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2023-12-31 00:00:00 ┆ 52           │
        ╰─────────────────────┴──────────────╯
        <BLANKLINE>
        (Showing first 4 of 4 rows)
    """
    return Expression._call_builtin_scalar_fn("week_of_year", expr)


def strftime(expr: Expression, format: str | None = None) -> Expression:
    """Converts a datetime/date column to a string column.

    Args:
        expr: The datetime or date expression to convert.
        format: The format to use for the conversion. If None, defaults to ISO 8601 format.

    Note:
        The format must be a valid datetime format string. (defaults to ISO 8601 format)
        See: https://docs.rs/chrono/latest/chrono/format/strftime/index.html


    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import strftime
        >>> df = daft.from_pydict(
        ...     {
        ...         "dates": [datetime.date(2023, 1, 1), datetime.date(2023, 1, 2), datetime.date(2023, 1, 3)],
        ...         "datetimes": [
        ...             datetime.datetime(2023, 1, 1, 12, 1),
        ...             datetime.datetime(2023, 1, 2, 12, 0, 0, 0),
        ...             datetime.datetime(2023, 1, 3, 12, 0, 0, 999_999),
        ...         ],
        ...     }
        ... )
        >>> df = df.with_column("datetimes_s", df["datetimes"].cast(daft.DataType.timestamp("s")))
        >>> df.select(
        ...     strftime(df["dates"]).alias("iso_date"),
        ...     strftime(df["dates"], format="%m/%d/%Y").alias("custom_date"),
        ...     strftime(df["datetimes"]).alias("iso_datetime"),
        ...     strftime(df["datetimes_s"]).alias("iso_datetime_s"),
        ...     strftime(df["datetimes_s"], format="%Y/%m/%d %H:%M:%S").alias("custom_datetime"),
        ... ).show()
        ╭────────────┬─────────────┬────────────────────────────┬─────────────────────┬─────────────────────╮
        │ iso_date   ┆ custom_date ┆ iso_datetime               ┆ iso_datetime_s      ┆ custom_datetime     │
        │ ---        ┆ ---         ┆ ---                        ┆ ---                 ┆ ---                 │
        │ String     ┆ String      ┆ String                     ┆ String              ┆ String              │
        ╞════════════╪═════════════╪════════════════════════════╪═════════════════════╪═════════════════════╡
        │ 2023-01-01 ┆ 01/01/2023  ┆ 2023-01-01T12:01:00        ┆ 2023-01-01T12:01:00 ┆ 2023/01/01 12:01:00 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2023-01-02 ┆ 01/02/2023  ┆ 2023-01-02T12:00:00        ┆ 2023-01-02T12:00:00 ┆ 2023/01/02 12:00:00 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2023-01-03 ┆ 01/03/2023  ┆ 2023-01-03T12:00:00.999999 ┆ 2023-01-03T12:00:00 ┆ 2023/01/03 12:00:00 │
        ╰────────────┴─────────────┴────────────────────────────┴─────────────────────┴─────────────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    return Expression._call_builtin_scalar_fn("strftime", expr, format)


def total_seconds(expr: Expression) -> Expression:
    """Calculates the total number of seconds for a duration column.

    Returns:
        Expression: a UInt64 expression with the total number of seconds for a duration column

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import total_seconds
        >>> df = daft.from_pydict(
        ...     {
        ...         "duration": [
        ...             datetime.timedelta(seconds=1),
        ...             datetime.timedelta(milliseconds=1),
        ...             datetime.timedelta(microseconds=1),
        ...             datetime.timedelta(days=1),
        ...             datetime.timedelta(hours=1),
        ...             datetime.timedelta(minutes=1),
        ...         ]
        ...     }
        ... )
        >>> df.with_column("Total Seconds", total_seconds(df["duration"])).show()
        ╭──────────────┬───────────────╮
        │ duration     ┆ Total Seconds │
        │ ---          ┆ ---           │
        │ Duration[us] ┆ Int64         │
        ╞══════════════╪═══════════════╡
        │ 1s           ┆ 1             │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1000µs       ┆ 0             │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1µs          ┆ 0             │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1d           ┆ 86400         │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1h           ┆ 3600          │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1m           ┆ 60            │
        ╰──────────────┴───────────────╯
        <BLANKLINE>
        (Showing first 6 of 6 rows)
    """
    return Expression._call_builtin_scalar_fn("total_seconds", expr)


def total_milliseconds(expr: Expression) -> Expression:
    """Calculates the total number of milliseconds for a duration column.

    Returns:
        Expression: a UInt64 expression with the total number of milliseconds for a duration column

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import total_milliseconds
        >>> df = daft.from_pydict(
        ...     {
        ...         "duration": [
        ...             datetime.timedelta(seconds=1),
        ...             datetime.timedelta(milliseconds=1),
        ...             datetime.timedelta(microseconds=1),
        ...             datetime.timedelta(days=1),
        ...             datetime.timedelta(hours=1),
        ...             datetime.timedelta(minutes=1),
        ...         ]
        ...     }
        ... )
        >>> df.with_column("Total Milliseconds", total_milliseconds(df["duration"])).show()
        ╭──────────────┬────────────────────╮
        │ duration     ┆ Total Milliseconds │
        │ ---          ┆ ---                │
        │ Duration[us] ┆ Int64              │
        ╞══════════════╪════════════════════╡
        │ 1s           ┆ 1000               │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1000µs       ┆ 1                  │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1µs          ┆ 0                  │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1d           ┆ 86400000           │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1h           ┆ 3600000            │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1m           ┆ 60000              │
        ╰──────────────┴────────────────────╯
        <BLANKLINE>
        (Showing first 6 of 6 rows)
    """
    return Expression._call_builtin_scalar_fn("total_milliseconds", expr)


def total_microseconds(expr: Expression) -> Expression:
    """Calculates the total number of microseconds for a duration column.

    Returns:
        Expression: a UInt64 expression with the total number of microseconds for a duration column

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import total_microseconds
        >>> df = daft.from_pydict(
        ...     {
        ...         "duration": [
        ...             datetime.timedelta(seconds=1),
        ...             datetime.timedelta(milliseconds=1),
        ...             datetime.timedelta(microseconds=1),
        ...             datetime.timedelta(days=1),
        ...             datetime.timedelta(hours=1),
        ...             datetime.timedelta(minutes=1),
        ...         ]
        ...     }
        ... )
        >>> df.with_column("Total Microseconds", total_microseconds(df["duration"])).show()
        ╭──────────────┬────────────────────╮
        │ duration     ┆ Total Microseconds │
        │ ---          ┆ ---                │
        │ Duration[us] ┆ Int64              │
        ╞══════════════╪════════════════════╡
        │ 1s           ┆ 1000000            │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1000µs       ┆ 1000               │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1µs          ┆ 1                  │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1d           ┆ 86400000000        │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1h           ┆ 3600000000         │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1m           ┆ 60000000           │
        ╰──────────────┴────────────────────╯
        <BLANKLINE>
        (Showing first 6 of 6 rows)
    """
    return Expression._call_builtin_scalar_fn("total_microseconds", expr)


def total_nanoseconds(expr: Expression) -> Expression:
    """Calculates the total number of nanoseconds for a duration column.

    Returns:
        Expression: a UInt64 expression with the total number of nanoseconds for a duration column

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import total_nanoseconds
        >>> df = daft.from_pydict(
        ...     {
        ...         "duration": [
        ...             datetime.timedelta(seconds=1),
        ...             datetime.timedelta(milliseconds=1),
        ...             datetime.timedelta(microseconds=1),
        ...             datetime.timedelta(days=1),
        ...             datetime.timedelta(hours=1),
        ...             datetime.timedelta(minutes=1),
        ...         ]
        ...     }
        ... )
        >>> df.with_column("Total Nanoseconds", total_nanoseconds(df["duration"])).show()
        ╭──────────────┬───────────────────╮
        │ duration     ┆ Total Nanoseconds │
        │ ---          ┆ ---               │
        │ Duration[us] ┆ Int64             │
        ╞══════════════╪═══════════════════╡
        │ 1s           ┆ 1000000000        │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1000µs       ┆ 1000000           │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1µs          ┆ 1000              │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1d           ┆ 86400000000000    │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1h           ┆ 3600000000000     │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1m           ┆ 60000000000       │
        ╰──────────────┴───────────────────╯
        <BLANKLINE>
        (Showing first 6 of 6 rows)
    """
    return Expression._call_builtin_scalar_fn("total_nanoseconds", expr)


def total_minutes(expr: Expression) -> Expression:
    """Calculates the total number of minutes for a duration column.

    Returns:
        Expression: a UInt64 expression with the total number of minutes for a duration column

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import total_minutes
        >>> df = daft.from_pydict(
        ...     {
        ...         "duration": [
        ...             datetime.timedelta(seconds=1),
        ...             datetime.timedelta(milliseconds=1),
        ...             datetime.timedelta(microseconds=1),
        ...             datetime.timedelta(days=1),
        ...             datetime.timedelta(hours=1),
        ...             datetime.timedelta(minutes=1),
        ...         ]
        ...     }
        ... )
        >>> df.with_column("Total Minutes", total_minutes(df["duration"])).show()
        ╭──────────────┬───────────────╮
        │ duration     ┆ Total Minutes │
        │ ---          ┆ ---           │
        │ Duration[us] ┆ Int64         │
        ╞══════════════╪═══════════════╡
        │ 1s           ┆ 0             │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1000µs       ┆ 0             │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1µs          ┆ 0             │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1d           ┆ 1440          │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1h           ┆ 60            │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1m           ┆ 1             │
        ╰──────────────┴───────────────╯
        <BLANKLINE>
        (Showing first 6 of 6 rows)
    """
    return Expression._call_builtin_scalar_fn("total_minutes", expr)


def total_hours(expr: Expression) -> Expression:
    """Calculates the total number of hours for a duration column.

    Returns:
        Expression: a UInt64 expression with the total number of hours for a duration column

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import total_hours
        >>> df = daft.from_pydict(
        ...     {
        ...         "duration": [
        ...             datetime.timedelta(seconds=1),
        ...             datetime.timedelta(milliseconds=1),
        ...             datetime.timedelta(microseconds=1),
        ...             datetime.timedelta(days=1),
        ...             datetime.timedelta(hours=1),
        ...             datetime.timedelta(minutes=1),
        ...         ]
        ...     }
        ... )
        >>> df.with_column("Total Hours", total_hours(df["duration"])).show()
        ╭──────────────┬─────────────╮
        │ duration     ┆ Total Hours │
        │ ---          ┆ ---         │
        │ Duration[us] ┆ Int64       │
        ╞══════════════╪═════════════╡
        │ 1s           ┆ 0           │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1000µs       ┆ 0           │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1µs          ┆ 0           │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1d           ┆ 24          │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1h           ┆ 1           │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1m           ┆ 0           │
        ╰──────────────┴─────────────╯
        <BLANKLINE>
        (Showing first 6 of 6 rows)
    """
    return Expression._call_builtin_scalar_fn("total_hours", expr)


def total_days(expr: Expression) -> Expression:
    """Calculates the total number of days for a duration column.

    Returns:
        Expression: a UInt64 expression with the total number of days for a duration column

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import total_days
        >>> df = daft.from_pydict(
        ...     {
        ...         "duration": [
        ...             datetime.timedelta(seconds=1),
        ...             datetime.timedelta(milliseconds=1),
        ...             datetime.timedelta(microseconds=1),
        ...             datetime.timedelta(days=1),
        ...             datetime.timedelta(hours=1),
        ...             datetime.timedelta(minutes=1),
        ...         ]
        ...     }
        ... )
        >>> df.with_column("Total Days", total_days(df["duration"])).show()
        ╭──────────────┬────────────╮
        │ duration     ┆ Total Days │
        │ ---          ┆ ---        │
        │ Duration[us] ┆ Int64      │
        ╞══════════════╪════════════╡
        │ 1s           ┆ 0          │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1000µs       ┆ 0          │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1µs          ┆ 0          │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1d           ┆ 1          │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1h           ┆ 0          │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 1m           ┆ 0          │
        ╰──────────────┴────────────╯
        <BLANKLINE>
        (Showing first 6 of 6 rows)
    """
    return Expression._call_builtin_scalar_fn("total_days", expr)


def to_date(expr: Expression, format: str) -> Expression:
    """Converts a string to a date using the specified format.

    Returns:
        Expression: a Date expression which is parsed by given format

    Note:
        The format must be a valid date format string. See: https://docs.rs/chrono/latest/chrono/format/strftime/index.html

    Examples:
        >>> import daft
        >>> from daft.functions import to_date
        >>> df = daft.from_pydict({"x": ["2021-01-01", "2021-01-02", None]})
        >>> df = df.with_column("date", to_date(df["x"], "%Y-%m-%d"))
        >>> df.show()
        ╭────────────┬────────────╮
        │ x          ┆ date       │
        │ ---        ┆ ---        │
        │ String     ┆ Date       │
        ╞════════════╪════════════╡
        │ 2021-01-01 ┆ 2021-01-01 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2021-01-02 ┆ 2021-01-02 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ None       ┆ None       │
        ╰────────────┴────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("to_date", expr, format=format)


def to_datetime(expr: Expression, format: str, timezone: str | None = None) -> Expression:
    """Converts a string to a datetime using the specified format and timezone.

    Returns:
        Expression: a DateTime expression which is parsed by given format and timezone

    Note:
        The format must be a valid datetime format string. See: https://docs.rs/chrono/latest/chrono/format/strftime/index.html

    Examples:
        >>> import daft
        >>> from daft.functions import to_datetime
        >>> df = daft.from_pydict({"x": ["2021-01-01 00:00:00.123", "2021-01-02 12:30:00.456", None]})
        >>> df = df.with_column("datetime", to_datetime(df["x"], "%Y-%m-%d %H:%M:%S%.3f"))
        >>> df.show()
        ╭─────────────────────────┬─────────────────────────╮
        │ x                       ┆ datetime                │
        │ ---                     ┆ ---                     │
        │ String                  ┆ Timestamp[ms]           │
        ╞═════════════════════════╪═════════════════════════╡
        │ 2021-01-01 00:00:00.123 ┆ 2021-01-01 00:00:00.123 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2021-01-02 12:30:00.456 ┆ 2021-01-02 12:30:00.456 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ None                    ┆ None                    │
        ╰─────────────────────────┴─────────────────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

        If a timezone is provided, the datetime will be parsed in that timezone

        >>> df = daft.from_pydict({"x": ["2021-01-01 00:00:00.123 +0800", "2021-01-02 12:30:00.456 +0800", None]})
        >>> df = df.with_column("datetime", to_datetime(df["x"], "%Y-%m-%d %H:%M:%S%.3f %z", timezone="Asia/Shanghai"))
        >>> df.show()
        ╭───────────────────────────────┬──────────────────────────────╮
        │ x                             ┆ datetime                     │
        │ ---                           ┆ ---                          │
        │ String                        ┆ Timestamp[ms; Asia/Shanghai] │
        ╞═══════════════════════════════╪══════════════════════════════╡
        │ 2021-01-01 00:00:00.123 +0800 ┆ 2021-01-01 00:00:00.123 CST  │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2021-01-02 12:30:00.456 +0800 ┆ 2021-01-02 12:30:00.456 CST  │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ None                          ┆ None                         │
        ╰───────────────────────────────┴──────────────────────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("to_datetime", expr, format=format, timezone=timezone)


def convert_time_zone(expr: Expression, to_timezone: str, from_timezone: str | None = None) -> Expression:
    """Converts a timestamp to another timezone while preserving the instant in time.

    If the timestamp has no timezone, `from_timezone` must be provided to interpret the local time before converting to `to_timezone`.

    Args:
        expr: Timestamp expression to convert.
        to_timezone: Target timezone (e.g. "UTC", "+02:00", "America/New_York").
        from_timezone: Source timezone for timestamps without a timezone.

    Returns:
        Expression: Timestamp expression with the target timezone.
    """
    return Expression._call_builtin_scalar_fn("convert_time_zone", expr, to_timezone, from_timezone)


def replace_time_zone(expr: Expression, timezone: str | None = None) -> Expression:
    """Replaces the timezone of a timestamp while preserving the local time.

    If `timezone` is not provided, the timezone is removed.

    Args:
        expr: Timestamp expression to update.
        timezone: New timezone (e.g. "UTC", "+02:00", "America/New_York").

    Returns:
        Expression: Timestamp expression with the updated timezone.
    """
    return Expression._call_builtin_scalar_fn("replace_time_zone", expr, timezone)


def date_trunc(interval: str, expr: Expression, relative_to: Expression | None = None) -> Expression:
    """Truncates the datetime column to the specified interval.

    Args:
        interval: The interval to truncate to. Must be a string representing a valid interval in "{integer} {unit}" format, e.g. "1 day". Valid time units are: 'microsecond', 'millisecond', 'second', 'minute', 'hour', 'day', 'week'.
        expr: The datetime expression to truncate.
        relative_to (optional): Timestamp to truncate relative to. If not provided, truncates to the start of the Unix epoch: 1970-01-01 00:00:00.

    Returns:
        Expression: a DateTime expression truncated to the specified interval

    Examples:
        >>> import datetime
        >>> import daft
        >>> from daft.functions import date_trunc
        >>>
        >>> df = daft.from_pydict(
        ...     {
        ...         "datetime": [
        ...             datetime.datetime(2021, 1, 1, 0, 1, 1),
        ...             datetime.datetime(2021, 1, 1, 0, 1, 59),
        ...             datetime.datetime(2021, 1, 1, 0, 2, 0),
        ...         ],
        ...     }
        ... )
        >>> df.with_column("truncated", date_trunc("1 minute", df["datetime"])).collect()
        ╭─────────────────────┬─────────────────────╮
        │ datetime            ┆ truncated           │
        │ ---                 ┆ ---                 │
        │ Timestamp[us]       ┆ Timestamp[us]       │
        ╞═════════════════════╪═════════════════════╡
        │ 2021-01-01 00:01:01 ┆ 2021-01-01 00:01:00 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2021-01-01 00:01:59 ┆ 2021-01-01 00:01:00 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2021-01-01 00:02:00 ┆ 2021-01-01 00:02:00 │
        ╰─────────────────────┴─────────────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("truncate", expr, relative_to, interval=interval)


def to_unix_epoch(expr: Expression, time_unit: str | TimeUnit | None = None) -> Expression:
    """Converts a datetime column to a Unix timestamp with the specified time unit. (default: seconds).

    See [daft.datatype.TimeUnit](https://docs.daft.ai/en/stable/api/datatypes/all_datatypes/#daft.datatype.DataType.timeunit) for more information on time units and valid values.

    Examples:
        >>> import daft
        >>> from datetime import date
        >>>
        >>> df = daft.from_pydict(
        ...     {
        ...         "dates": [
        ...             date(2001, 1, 1),
        ...             date(2001, 1, 2),
        ...             date(2001, 1, 3),
        ...             None,
        ...         ]
        ...     }
        ... )
        >>> df.with_column("timestamp", df["dates"].to_unix_epoch("ns")).show()
        ╭────────────┬────────────────────╮
        │ dates      ┆ timestamp          │
        │ ---        ┆ ---                │
        │ Date       ┆ Int64              │
        ╞════════════╪════════════════════╡
        │ 2001-01-01 ┆ 978307200000000000 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2001-01-02 ┆ 978393600000000000 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2001-01-03 ┆ 978480000000000000 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ None       ┆ None               │
        ╰────────────┴────────────────────╯
        <BLANKLINE>
        (Showing first 4 of 4 rows)
    """
    return Expression._call_builtin_scalar_fn("to_unix_epoch", expr, time_unit=time_unit)


def current_date() -> Expression:
    """Returns the current date (UTC).

    Returns:
        Expression: a Date expression containing today's date, broadcast to every row.

    Examples:
        >>> import daft
        >>> from daft.functions import current_date
        >>> df = daft.from_pydict({"x": [1, 2, 3]})
        >>> df = df.with_column("today", current_date())
        >>> df.schema()["today"].dtype == daft.DataType.date()
        True
    """
    return Expression._call_builtin_scalar_fn("current_date")


def current_timestamp() -> Expression:
    """Returns the current timestamp (UTC) with microsecond precision.

    Returns:
        Expression: a Timestamp[us] expression containing the current datetime, broadcast to every row.

    Examples:
        >>> import daft
        >>> from daft.functions import current_timestamp
        >>> df = daft.from_pydict({"x": [1, 2, 3]})
        >>> df = df.with_column("now", current_timestamp())
        >>> df.schema()["now"].dtype == daft.DataType.timestamp("us")
        True
    """
    return Expression._call_builtin_scalar_fn("current_timestamp")


def current_timezone() -> Expression:
    """Returns the current timezone as a string (always 'UTC' in Daft).

    Returns:
        Expression: a String expression containing 'UTC', broadcast to every row.

    Examples:
        >>> import daft
        >>> from daft.functions import current_timezone
        >>> df = daft.from_pydict({"x": [1, 2, 3]})
        >>> df = df.with_column("tz", current_timezone())
        >>> df.schema()["tz"].dtype == daft.DataType.string()
        True
    """
    return Expression._call_builtin_scalar_fn("current_timezone")


def date_add(expr: Expression, days: Expression) -> Expression:
    """Adds a number of days to a date.

    Args:
        expr: A date expression.
        days: An integer expression representing the number of days to add.

    Returns:
        Expression: a Date expression.

    Examples:
        >>> import daft
        >>> from daft.functions import date_add
        >>> df = daft.from_pydict({"d": ["2021-01-01", "2021-06-15"], "n": [10, 5]})
        >>> df = df.with_column("d", df["d"].cast(daft.DataType.date()))
        >>> df = df.with_column("result", date_add(df["d"], df["n"]))
        >>> df.schema()["result"].dtype == daft.DataType.date()
        True
    """
    return Expression._call_builtin_scalar_fn("date_add", expr, days)


def date_sub(expr: Expression, days: Expression) -> Expression:
    """Subtracts a number of days from a date.

    Args:
        expr: A date expression.
        days: An integer expression representing the number of days to subtract.

    Returns:
        Expression: a Date expression.

    Examples:
        >>> import daft
        >>> from daft.functions import date_sub
        >>> df = daft.from_pydict({"d": ["2021-01-10", "2021-06-15"], "n": [5, 10]})
        >>> df = df.with_column("d", df["d"].cast(daft.DataType.date()))
        >>> df = df.with_column("result", date_sub(df["d"], df["n"]))
        >>> df.schema()["result"].dtype == daft.DataType.date()
        True
    """
    return Expression._call_builtin_scalar_fn("date_sub", expr, days)


def date_diff(end: Expression, start: Expression) -> Expression:
    """Returns the number of days between two dates.

    Args:
        end: The end date or timestamp expression.
        start: The start date or timestamp expression.

    Returns:
        Expression: an Int32 expression with the number of days (end - start).

    Examples:
        >>> import daft
        >>> from daft.functions import date_diff
        >>> df = daft.from_pydict({"a": ["2021-01-10"], "b": ["2021-01-01"]})
        >>> df = df.with_column("a", df["a"].cast(daft.DataType.date()))
        >>> df = df.with_column("b", df["b"].cast(daft.DataType.date()))
        >>> df = df.with_column("diff", date_diff(df["a"], df["b"]))
        >>> df.schema()["diff"].dtype == daft.DataType.int32()
        True
    """
    return Expression._call_builtin_scalar_fn("date_diff", end, start)


def date_from_unix_date(expr: Expression) -> Expression:
    """Converts days since Unix epoch (1970-01-01) to a date.

    Args:
        expr: An integer expression representing days since epoch.

    Returns:
        Expression: a Date expression.

    Examples:
        >>> import daft
        >>> from daft.functions import date_from_unix_date
        >>> df = daft.from_pydict({"days": [0, 18628]})
        >>> df = df.with_column("d", date_from_unix_date(df["days"]))
        >>> df.schema()["d"].dtype == daft.DataType.date()
        True
    """
    return Expression._call_builtin_scalar_fn("date_from_unix_date", expr)


def timestamp_seconds(expr: Expression) -> Expression:
    """Creates a timestamp from seconds since Unix epoch.

    Args:
        expr: A numeric expression representing seconds since epoch.

    Returns:
        Expression: a Timestamp[us] expression.

    Examples:
        >>> import daft
        >>> from daft.functions import timestamp_seconds
        >>> df = daft.from_pydict({"s": [0, 1609459200]})
        >>> df = df.with_column("ts", timestamp_seconds(df["s"]))
        >>> df.schema()["ts"].dtype == daft.DataType.timestamp("us")
        True
    """
    return Expression._call_builtin_scalar_fn("timestamp_seconds", expr)


def timestamp_millis(expr: Expression) -> Expression:
    """Creates a timestamp from milliseconds since Unix epoch.

    Args:
        expr: A numeric expression representing milliseconds since epoch.

    Returns:
        Expression: a Timestamp[us] expression.

    Examples:
        >>> import daft
        >>> from daft.functions import timestamp_millis
        >>> df = daft.from_pydict({"ms": [0, 1609459200000]})
        >>> df = df.with_column("ts", timestamp_millis(df["ms"]))
        >>> df.schema()["ts"].dtype == daft.DataType.timestamp("us")
        True
    """
    return Expression._call_builtin_scalar_fn("timestamp_millis", expr)


def timestamp_micros(expr: Expression) -> Expression:
    """Creates a timestamp from microseconds since Unix epoch.

    Args:
        expr: A numeric expression representing microseconds since epoch.

    Returns:
        Expression: a Timestamp[us] expression.

    Examples:
        >>> import daft
        >>> from daft.functions import timestamp_micros
        >>> df = daft.from_pydict({"us": [0, 1609459200000000]})
        >>> df = df.with_column("ts", timestamp_micros(df["us"]))
        >>> df.schema()["ts"].dtype == daft.DataType.timestamp("us")
        True
    """
    return Expression._call_builtin_scalar_fn("timestamp_micros", expr)


def from_unixtime(expr: Expression, format: str | None = None) -> Expression:
    """Converts a Unix timestamp (seconds) to a formatted string.

    Args:
        expr: A numeric expression representing seconds since epoch.
        format: Optional strftime format string. Defaults to '%Y-%m-%d %H:%M:%S'.

    Returns:
        Expression: a Utf8 expression with the formatted timestamp.

    Examples:
        >>> import daft
        >>> from daft.functions import from_unixtime
        >>> df = daft.from_pydict({"s": [0, 1609459200]})
        >>> df = df.with_column("formatted", from_unixtime(df["s"]))
        >>> df.schema()["formatted"].dtype == daft.DataType.string()
        True
    """
    if format is not None:
        return Expression._call_builtin_scalar_fn("from_unixtime", expr, format=format)
    return Expression._call_builtin_scalar_fn("from_unixtime", expr)


def make_date(year: Expression, month: Expression, day: Expression) -> Expression:
    """Creates a date from year, month, and day integer components.

    Invalid dates (e.g., Feb 30) return null.

    Args:
        year: integer expression for the year.
        month: integer expression for the month (1-12).
        day: integer expression for the day (1-31).

    Returns:
        Expression: a Date expression.

    Examples:
        >>> import daft
        >>> from daft.functions import make_date
        >>> make_date(daft.col("y"), daft.col("m"), daft.col("d"))
        make_date(col(y), col(m), col(d))
    """
    year = Expression._to_expression(year)
    month = Expression._to_expression(month)
    day = Expression._to_expression(day)
    return Expression._call_builtin_scalar_fn("make_date", year, month, day)


def make_timestamp(
    year: Expression,
    month: Expression,
    day: Expression,
    hour: Expression,
    minute: Expression,
    second: Expression,
    timezone: str | None = None,
) -> Expression:
    """Creates a timestamp from individual date/time components.

    The ``second`` parameter accepts fractional values for sub-second precision.
    Invalid component combinations return null.

    Args:
        year: integer expression for the year.
        month: integer expression for the month (1-12).
        day: integer expression for the day (1-31).
        hour: integer expression for the hour (0-23).
        minute: integer expression for the minute (0-59).
        second: numeric expression for the second (0-59, may include fractional part).
        timezone: optional timezone string (e.g. ``"UTC"``). When provided the
            returned timestamp carries this timezone metadata.

    Returns:
        Expression: a Timestamp(microseconds) expression.

    Examples:
        >>> import daft
        >>> from daft.functions import make_timestamp
        >>> make_timestamp(daft.col("y"), daft.col("m"), daft.col("d"), daft.col("h"), daft.col("mi"), daft.col("s"))
        make_timestamp(col(y), col(m), col(d), col(h), col(mi), col(s))
    """
    year = Expression._to_expression(year)
    month = Expression._to_expression(month)
    day = Expression._to_expression(day)
    hour = Expression._to_expression(hour)
    minute = Expression._to_expression(minute)
    second = Expression._to_expression(second)
    return Expression._call_builtin_scalar_fn(
        "make_timestamp", year, month, day, hour, minute, second, timezone=timezone
    )


def make_timestamp_ltz(
    year: Expression,
    month: Expression,
    day: Expression,
    hour: Expression,
    minute: Expression,
    second: Expression,
    timezone: str | None = None,
) -> Expression:
    """Creates a UTC timestamp from individual date/time components.

    When ``timezone`` is provided, the components are interpreted in that
    timezone and converted to UTC. Without a timezone the components are
    treated as UTC directly.

    Args:
        year: integer expression for the year.
        month: integer expression for the month (1-12).
        day: integer expression for the day (1-31).
        hour: integer expression for the hour (0-23).
        minute: integer expression for the minute (0-59).
        second: numeric expression for the second (0-59, may include fractional part).
        timezone: optional source timezone string (e.g. ``"US/Eastern"``).

    Returns:
        Expression: a Timestamp(microseconds, UTC) expression.

    Examples:
        >>> import daft
        >>> from daft.functions import make_timestamp_ltz
        >>> make_timestamp_ltz(
        ...     daft.col("y"), daft.col("m"), daft.col("d"), daft.col("h"), daft.col("mi"), daft.col("s")
        ... )
        make_timestamp_ltz(col(y), col(m), col(d), col(h), col(mi), col(s))
    """
    year = Expression._to_expression(year)
    month = Expression._to_expression(month)
    day = Expression._to_expression(day)
    hour = Expression._to_expression(hour)
    minute = Expression._to_expression(minute)
    second = Expression._to_expression(second)
    return Expression._call_builtin_scalar_fn(
        "make_timestamp_ltz", year, month, day, hour, minute, second, timezone=timezone
    )


def last_day(expr: Expression) -> Expression:
    """Returns the last day of the month for the given date or timestamp.

    Args:
        expr: a Date or Timestamp expression.

    Returns:
        Expression: a Date expression representing the last day of that month.

    Examples:
        >>> import daft
        >>> from daft.functions import last_day
        >>> last_day(daft.col("dt"))
        last_day(col(dt))
    """
    expr = Expression._to_expression(expr)
    return Expression._call_builtin_scalar_fn("last_day", expr)


def next_day(expr: Expression, day_of_week: str) -> Expression:
    """Returns the next occurrence of the specified day of the week after the given date.

    Args:
        expr: a Date or Timestamp expression.
        day_of_week: the target weekday (e.g. ``"Monday"``, ``"Mon"``).

    Returns:
        Expression: a Date expression for the next occurrence of that weekday.

    Examples:
        >>> import daft
        >>> from daft.functions import next_day
        >>> next_day(daft.col("dt"), "Monday")
        next_day(col(dt), lit("Monday"))
    """
    expr = Expression._to_expression(expr)
    return Expression._call_builtin_scalar_fn("next_day", expr, day_of_week=day_of_week)
