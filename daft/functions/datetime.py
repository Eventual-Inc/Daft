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
        ...             datetime.datetime(2024, 12, 31, 0, 0, 0),  # part of week 1 of 2025 according to ISO 8601 standard
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
        │ 2023-01-01 ┆ 01/01/2023  ┆ 2023-01-01T12:01:00.000000 ┆ 2023-01-01T12:01:00 ┆ 2023/01/01 12:01:00 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2023-01-02 ┆ 01/02/2023  ┆ 2023-01-02T12:00:00.000000 ┆ 2023-01-02T12:00:00 ┆ 2023/01/02 12:00:00 │
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

    See [daft.datatype.TimeUnit](https://docs.daft.ai/en/stable/api/datatypes/#daft.datatype.DataType.timeunit) for more information on time units and valid values.

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
