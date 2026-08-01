# functions datetime

## add_months

```python
add_months(expr: Expression, months: Expression) -> Expression
```

Adds a number of months to a date or timestamp.

Mirrors Spark's ``add_months``: when the start day exceeds the number of days in
the resulting month, the result is clamped to the last day of that month. The
return type is always Date, even when the input is a Timestamp (the time-of-day
component is dropped before the shift).

Args:
    expr: A Date or Timestamp expression.
    months: An integer expression for the number of months to add (may be negative).

Returns:
    Expression: a Date expression shifted by the given number of months.

## convert_time_zone

```python
convert_time_zone(expr: Expression, to_timezone: str, from_timezone: str | None=None) -> Expression
```

Converts a timestamp to another timezone while preserving the instant in time.

If the timestamp has no timezone, `from_timezone` must be provided to interpret the local time before converting to `to_timezone`.

Args:
    expr: Timestamp expression to convert.
    to_timezone: Target timezone (e.g. "UTC", "+02:00", "America/New_York").
    from_timezone: Source timezone for timestamps without a timezone.

Returns:
    Expression: Timestamp expression with the target timezone.

## convert_timezone

```python
convert_timezone(target_timezone: str, source_timestamp: Expression) -> Expression
```

Spark-style alias for :func:`convert_time_zone`.

Note Spark's argument order is ``(target_timezone, source_timestamp)`` which is the
reverse of Daft's :func:`convert_time_zone`. The source timestamp must already carry
a timezone (this alias does not accept a ``from_timezone`` argument).

Args:
    target_timezone: Target timezone name.
    source_timestamp: A tz-aware Timestamp expression.

Returns:
    Expression: Timestamp expression in the target timezone.

## current_date

```python
current_date() -> Expression
```

Returns the current date (UTC).

Returns:
    Expression: a Date expression containing today's date, broadcast to every row.

## current_timestamp

```python
current_timestamp() -> Expression
```

Returns the current timestamp (UTC) with microsecond precision.

Returns:
    Expression: a Timestamp[us] expression containing the current datetime, broadcast to every row.

## current_timezone

```python
current_timezone() -> Expression
```

Returns the current timezone as a string (always 'UTC' in Daft).

Returns:
    Expression: a String expression containing 'UTC', broadcast to every row.

## date

```python
date(expr: Expression) -> Expression
```

Retrieves the date for a datetime column.

Returns:
    Expression: a Date expression

## date_add

```python
date_add(expr: Expression, days: Expression) -> Expression
```

Adds a number of days to a date.

Args:
    expr: A date expression.
    days: An integer expression representing the number of days to add.

Returns:
    Expression: a Date expression.

## date_diff

```python
date_diff(end: Expression, start: Expression) -> Expression
```

Returns the number of days between two dates.

Args:
    end: The end date or timestamp expression.
    start: The start date or timestamp expression.

Returns:
    Expression: an Int32 expression with the number of days (end - start).

## date_format

```python
date_format(expr: Expression, format: str | None=None) -> Expression
```

Alias for ``strftime``.

## date_from_unix_date

```python
date_from_unix_date(expr: Expression) -> Expression
```

Converts days since Unix epoch (1970-01-01) to a date.

Args:
    expr: An integer expression representing days since epoch.

Returns:
    Expression: a Date expression.

## date_sub

```python
date_sub(expr: Expression, days: Expression) -> Expression
```

Subtracts a number of days from a date.

Args:
    expr: A date expression.
    days: An integer expression representing the number of days to subtract.

Returns:
    Expression: a Date expression.

## date_trunc

```python
date_trunc(interval: str, expr: Expression, relative_to: Expression | None=None) -> Expression
```

Truncates the datetime column to the specified interval.

Args:
    interval: The interval to truncate to. Must be a string representing a valid interval in "{integer} {unit}" format, e.g. "1 day". Valid time units are: 'microsecond', 'millisecond', 'second', 'minute', 'hour', 'day', 'week'.
    expr: The datetime expression to truncate.
    relative_to (optional): Timestamp to truncate relative to. If not provided, truncates to the start of the Unix epoch: 1970-01-01 00:00:00.

Returns:
    Expression: a DateTime expression truncated to the specified interval

## dateadd

```python
dateadd(expr: Expression, days: Expression) -> Expression
```

Alias for ``date_add``.

## datediff

```python
datediff(end: Expression, start: Expression) -> Expression
```

Alias for ``date_diff``.

## datepart

```python
datepart(part: str, expr: Expression) -> Expression
```

Alias-style extractor over existing temporal functions.

Args:
    part: Date part name, e.g. ``"year"``, ``"dayofmonth"``, ``"weekofyear"``.
    expr: Temporal expression to extract from.

## day

```python
day(expr: Expression) -> Expression
```

Retrieves the day for a datetime column.

Returns:
    Expression: a UInt32 expression with just the day extracted from a datetime column

## day_of_month

```python
day_of_month(expr: Expression) -> Expression
```

Retrieves the day of the month for a datetime column.

Returns:
    Expression: a UInt32 expression with just the day_of_month extracted from a datetime column

## day_of_week

```python
day_of_week(expr: Expression) -> Expression
```

Retrieves the day of the week for a datetime column, starting at 0 for Monday and ending at 6 for Sunday.

Returns:
    Expression: a UInt32 expression with just the day_of_week extracted from a datetime column

## day_of_year

```python
day_of_year(expr: Expression) -> Expression
```

Retrieves the ordinal day for a datetime column. Starting at 1 for January 1st and ending at 365 or 366 for December 31st.

Returns:
    Expression: a UInt32 expression with just the day_of_year extracted from a datetime column

## dayofmonth

```python
dayofmonth(expr: Expression) -> Expression
```

Alias for ``day_of_month``.

## dayofyear

```python
dayofyear(expr: Expression) -> Expression
```

Alias for ``day_of_year``.

## from_unixtime

```python
from_unixtime(expr: Expression, format: str | None=None) -> Expression
```

Converts a Unix timestamp (seconds) to a formatted string.

Args:
    expr: A numeric expression representing seconds since epoch.
    format: Optional strftime format string. Defaults to '%Y-%m-%d %H:%M:%S'.

Returns:
    Expression: a Utf8 expression with the formatted timestamp.

## from_utc_timestamp

```python
from_utc_timestamp(expr: Expression, timezone: str) -> Expression
```

Interprets a UTC timestamp and returns the wall-clock time in the given timezone.

Mirrors Spark's ``from_utc_timestamp``. The input is treated as a UTC instant
regardless of any timezone label, and the result is a tz-naive Timestamp whose
value reads as the wall-clock time in ``timezone``.

Args:
    expr: A Timestamp expression interpreted as UTC.
    timezone: Target timezone name (e.g. ``"America/Los_Angeles"`` or ``"+01:00"``).

Returns:
    Expression: A tz-naive Timestamp expression representing the wall-clock in ``timezone``.

Note:
    Unlike Spark, Daft does not silently resolve DST transitions during this
    conversion. ``from_utc_timestamp`` itself maps UTC instants to local time
    via :func:`chrono::TimeZone::from_utc_datetime`, which is unambiguous and
    never errors. The strict DST handling only applies to :func:`to_utc_timestamp`
    (see its docstring).

## hour

```python
hour(expr: Expression) -> Expression
```

Retrieves the hour for a datetime column.

Returns:
    Expression: a UInt32 expression with just the hour extracted from a datetime column

## last_day

```python
last_day(expr: Expression) -> Expression
```

Returns the last day of the month for the given date or timestamp.

Args:
    expr: a Date or Timestamp expression.

Returns:
    Expression: a Date expression representing the last day of that month.

## make_date

```python
make_date(year: Expression, month: Expression, day: Expression) -> Expression
```

Creates a date from year, month, and day integer components.

Invalid dates (e.g., Feb 30) return null.

Args:
    year: integer expression for the year.
    month: integer expression for the month (1-12).
    day: integer expression for the day (1-31).

Returns:
    Expression: a Date expression.

## make_timestamp

```python
make_timestamp(year: Expression, month: Expression, day: Expression, hour: Expression, minute: Expression, second: Expression, timezone: str | None=None) -> Expression
```

Creates a timestamp from individual date/time components.

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

## make_timestamp_ltz

```python
make_timestamp_ltz(year: Expression, month: Expression, day: Expression, hour: Expression, minute: Expression, second: Expression, timezone: str | None=None) -> Expression
```

Creates a UTC timestamp from individual date/time components.

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

## microsecond

```python
microsecond(expr: Expression) -> Expression
```

Retrieves the microsecond for a datetime column.

## millisecond

```python
millisecond(expr: Expression) -> Expression
```

Retrieves the millisecond for a datetime column.

## minute

```python
minute(expr: Expression) -> Expression
```

Retrieves the minute for a datetime column.

Returns:
    Expression: a UInt32 expression with just the minute extracted from a datetime column

## month

```python
month(expr: Expression) -> Expression
```

Retrieves the month for a datetime column.

Returns:
    Expression: a UInt32 expression with just the month extracted from a datetime column

## months_between

```python
months_between(end: Expression, start: Expression) -> Expression
```

Returns the number of months between two dates or timestamps.

Mirrors Spark's ``months_between``: returns an integer when both inputs share the
same day-of-month or are both the last day of their respective months; otherwise
returns ``months_diff + (day1 - day2 + (time1 - time2)/86400) / 31`` rounded to
eight decimal places.

Args:
    end: The end Date or Timestamp expression.
    start: The start Date or Timestamp expression.

Returns:
    Expression: a Float64 expression with the number of months (end - start).

## nanosecond

```python
nanosecond(expr: Expression) -> Expression
```

Retrieves the nanosecond for a datetime column.

## next_day

```python
next_day(expr: Expression, day_of_week: str) -> Expression
```

Returns the next occurrence of the specified day of the week after the given date.

Args:
    expr: a Date or Timestamp expression.
    day_of_week: the target weekday (e.g. ``"Monday"``, ``"Mon"``).

Returns:
    Expression: a Date expression for the next occurrence of that weekday.

## quarter

```python
quarter(expr: Expression) -> Expression
```

Retrieves the quarter for a datetime column.

Returns:
    Expression: a UInt32 expression with just the quarter extracted from a datetime column

## replace_time_zone

```python
replace_time_zone(expr: Expression, timezone: str | None=None) -> Expression
```

Replaces the timezone of a timestamp while preserving the local time.

If `timezone` is not provided, the timezone is removed.

Args:
    expr: Timestamp expression to update.
    timezone: New timezone (e.g. "UTC", "+02:00", "America/New_York").

Returns:
    Expression: Timestamp expression with the updated timezone.

## second

```python
second(expr: Expression) -> Expression
```

Retrieves the second for a datetime column.

Returns:
    Expression: a UInt32 expression with just the second extracted from a datetime column

## strftime

```python
strftime(expr: Expression, format: str | None=None) -> Expression
```

Converts a datetime/date column to a string column.

Args:
    expr: The datetime or date expression to convert.
    format: The format to use for the conversion. If None, defaults to ISO 8601 format.

Note:
    The format must be a valid datetime format string. (defaults to ISO 8601 format)
    See: https://docs.rs/chrono/latest/chrono/format/strftime/index.html

## time

```python
time(expr: Expression) -> Expression
```

Retrieves the time for a datetime column.

Returns:
    Expression: a Time expression

## timestamp_micros

```python
timestamp_micros(expr: Expression) -> Expression
```

Creates a timestamp from microseconds since Unix epoch.

Args:
    expr: A numeric expression representing microseconds since epoch.

Returns:
    Expression: a Timestamp[us] expression.

## timestamp_millis

```python
timestamp_millis(expr: Expression) -> Expression
```

Creates a timestamp from milliseconds since Unix epoch.

Args:
    expr: A numeric expression representing milliseconds since epoch.

Returns:
    Expression: a Timestamp[us] expression.

## timestamp_seconds

```python
timestamp_seconds(expr: Expression) -> Expression
```

Creates a timestamp from seconds since Unix epoch.

Args:
    expr: A numeric expression representing seconds since epoch.

Returns:
    Expression: a Timestamp[us] expression.

## to_date

```python
to_date(expr: Expression, format: str) -> Expression
```

Converts a string to a date using the specified format.

Returns:
    Expression: a Date expression which is parsed by given format

Note:
    The format must be a valid date format string. See: https://docs.rs/chrono/latest/chrono/format/strftime/index.html

## to_datetime

```python
to_datetime(expr: Expression, format: str, timezone: str | None=None) -> Expression
```

Converts a string to a datetime using the specified format and timezone.

Returns:
    Expression: a DateTime expression which is parsed by given format and timezone

Note:
    The format must be a valid datetime format string. See: https://docs.rs/chrono/latest/chrono/format/strftime/index.html

## to_unix_epoch

```python
to_unix_epoch(expr: Expression, time_unit: str | TimeUnit | None=None) -> Expression
```

Converts a datetime column to a Unix timestamp with the specified time unit. (default: seconds).

See [daft.datatype.TimeUnit](https://docs.daft.ai/en/stable/api/datatypes/all_datatypes/#daft.datatype.DataType.timeunit) for more information on time units and valid values.

## to_utc_timestamp

```python
to_utc_timestamp(expr: Expression, timezone: str) -> Expression
```

Interprets a wall-clock timestamp in the given timezone and returns the UTC instant.

Mirrors Spark's ``to_utc_timestamp``. The input's wall-clock value is treated as
local time in ``timezone`` and converted to the equivalent UTC instant, returned as
a tz-naive Timestamp.

Args:
    expr: A Timestamp expression whose wall-clock is interpreted in ``timezone``.
    timezone: Source timezone name.

Returns:
    Expression: A tz-naive Timestamp expression representing the UTC instant.

Note:
    DST transition handling differs from Spark. When the local wall-clock falls
    in a non-existent gap (e.g. the spring-forward hour) or an ambiguous overlap
    (e.g. the fall-back hour), Daft raises a ``ValueError`` rather than silently
    picking a side. Spark instead advances past the gap and resolves ambiguity
    to the pre-transition offset. If you need Spark-compatible behavior, filter
    or pre-shift these inputs before calling.

## total_days

```python
total_days(expr: Expression) -> Expression
```

Calculates the total number of days for a duration column.

Returns:
    Expression: a UInt64 expression with the total number of days for a duration column

## total_hours

```python
total_hours(expr: Expression) -> Expression
```

Calculates the total number of hours for a duration column.

Returns:
    Expression: a UInt64 expression with the total number of hours for a duration column

## total_microseconds

```python
total_microseconds(expr: Expression) -> Expression
```

Calculates the total number of microseconds for a duration column.

Returns:
    Expression: a UInt64 expression with the total number of microseconds for a duration column

## total_milliseconds

```python
total_milliseconds(expr: Expression) -> Expression
```

Calculates the total number of milliseconds for a duration column.

Returns:
    Expression: a UInt64 expression with the total number of milliseconds for a duration column

## total_minutes

```python
total_minutes(expr: Expression) -> Expression
```

Calculates the total number of minutes for a duration column.

Returns:
    Expression: a UInt64 expression with the total number of minutes for a duration column

## total_nanoseconds

```python
total_nanoseconds(expr: Expression) -> Expression
```

Calculates the total number of nanoseconds for a duration column.

Returns:
    Expression: a UInt64 expression with the total number of nanoseconds for a duration column

## total_seconds

```python
total_seconds(expr: Expression) -> Expression
```

Calculates the total number of seconds for a duration column.

Returns:
    Expression: a UInt64 expression with the total number of seconds for a duration column

## trunc

```python
trunc(expr: Expression, interval: str, relative_to: Expression | None=None) -> Expression
```

Alias for ``date_trunc`` with Spark-style argument order.

Args:
    expr: The datetime/date expression to truncate.
    interval: The truncation unit/interval (e.g. ``"day"``, ``"month"``, ``"1 hour"``).
    relative_to (optional): Timestamp to truncate relative to.

## unix_date

```python
unix_date(expr: Expression) -> Expression
```

Retrieves the number of days since 1970-01-01 00:00:00 UTC.

Returns:
    Expression: a UInt64 expression

## week_of_year

```python
week_of_year(expr: Expression) -> Expression
```

Retrieves the week of the year for a datetime column.

Returns:
    Expression: a UInt32 expression with just the week_of_year extracted from a datetime column

## weekofyear

```python
weekofyear(expr: Expression) -> Expression
```

Alias for ``week_of_year``.

## year

```python
year(expr: Expression) -> Expression
```

Retrieves the year for a datetime column.

Returns:
    Expression: a Int32 expression with just the year extracted from a datetime column
