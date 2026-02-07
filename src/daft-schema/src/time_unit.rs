use std::{fmt::Display, str::FromStr};

use arrow_schema::TimeUnit as ArrowTimeUnit;
use chrono::{LocalResult, TimeZone};
use common_error::DaftError;
use daft_arrow::{datatypes::TimeUnit as Arrow2TimeUnit, error::Error};
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub enum TimeUnit {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

impl Display for TimeUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Nanoseconds => write!(f, "ns"),
            Self::Microseconds => write!(f, "us"),
            Self::Milliseconds => write!(f, "ms"),
            Self::Seconds => write!(f, "s"),
        }
    }
}

impl TimeUnit {
    #![allow(clippy::wrong_self_convention)]
    #[must_use]
    #[deprecated(note = "use .to_arrow")]
    pub fn to_arrow2(&self) -> Arrow2TimeUnit {
        match self {
            Self::Nanoseconds => Arrow2TimeUnit::Nanosecond,
            Self::Microseconds => Arrow2TimeUnit::Microsecond,
            Self::Milliseconds => Arrow2TimeUnit::Millisecond,
            Self::Seconds => Arrow2TimeUnit::Second,
        }
    }
    #[must_use]
    pub fn to_arrow(&self) -> ArrowTimeUnit {
        match self {
            Self::Nanoseconds => ArrowTimeUnit::Nanosecond,
            Self::Microseconds => ArrowTimeUnit::Microsecond,
            Self::Milliseconds => ArrowTimeUnit::Millisecond,
            Self::Seconds => ArrowTimeUnit::Second,
        }
    }

    #[must_use]
    pub fn to_scale_factor(&self) -> i64 {
        match self {
            Self::Seconds => 1,
            Self::Milliseconds => 1000,
            Self::Microseconds => 1_000_000,
            Self::Nanoseconds => 1_000_000_000,
        }
    }
}

impl FromStr for TimeUnit {
    type Err = DaftError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ns" | "nanoseconds" => Ok(Self::Nanoseconds),
            "us" | "microseconds" => Ok(Self::Microseconds),
            "ms" | "milliseconds" => Ok(Self::Milliseconds),
            "s" | "seconds" => Ok(Self::Seconds),
            _ => Err(DaftError::ValueError("Invalid time unit".to_string())),
        }
    }
}

impl From<&Arrow2TimeUnit> for TimeUnit {
    fn from(tu: &Arrow2TimeUnit) -> Self {
        match tu {
            Arrow2TimeUnit::Nanosecond => Self::Nanoseconds,
            Arrow2TimeUnit::Microsecond => Self::Microseconds,
            Arrow2TimeUnit::Millisecond => Self::Milliseconds,
            Arrow2TimeUnit::Second => Self::Seconds,
        }
    }
}

impl From<&ArrowTimeUnit> for TimeUnit {
    fn from(tu: &ArrowTimeUnit) -> Self {
        match tu {
            ArrowTimeUnit::Nanosecond => Self::Nanoseconds,
            ArrowTimeUnit::Microsecond => Self::Microseconds,
            ArrowTimeUnit::Millisecond => Self::Milliseconds,
            ArrowTimeUnit::Second => Self::Seconds,
        }
    }
}

#[must_use]
pub fn infer_timeunit_from_format_string(format: &str) -> TimeUnit {
    if format.contains("%9f") || format.contains("%.9f") {
        TimeUnit::Nanoseconds
    } else if format.contains("%3f") || format.contains("%.3f") {
        TimeUnit::Milliseconds
    } else {
        TimeUnit::Microseconds
    }
}

#[must_use]
pub fn format_string_has_offset(format: &str) -> bool {
    // These are all valid chrono formats that contain an offset
    format.contains("%Z")
        || format.contains("%z")
        || format.contains("%:z")
        || format.contains("%::z")
        || format.contains("%#z")
        || format == "%+"
}

/// Converts a timestamp in `time_unit` into [`chrono::NaiveDateTime`].
#[inline]
pub fn timestamp_to_naive_datetime(timestamp: i64, time_unit: TimeUnit) -> chrono::NaiveDateTime {
    match time_unit {
        TimeUnit::Seconds => daft_arrow::temporal_conversions::timestamp_s_to_datetime(timestamp)
            .expect("timestamp_s_to_datetime should not return None"),
        TimeUnit::Milliseconds => {
            daft_arrow::temporal_conversions::timestamp_ms_to_datetime(timestamp)
                .expect("timestamp_ms_to_datetime should not return None")
        }
        TimeUnit::Microseconds => {
            daft_arrow::temporal_conversions::timestamp_us_to_datetime(timestamp)
                .expect("timestamp_us_to_datetime should not return None")
        }
        TimeUnit::Nanoseconds => {
            daft_arrow::temporal_conversions::timestamp_ns_to_datetime(timestamp)
                .expect("timestamp_ns_to_datetime should not return None")
        }
    }
}

/// Converts a timestamp in `time_unit` into a [`chrono::DateTime`] in `timezone`.
#[inline]
pub fn timestamp_to_datetime<T: chrono::TimeZone>(
    timestamp: i64,
    time_unit: TimeUnit,
    timezone: &T,
) -> chrono::DateTime<T> {
    timezone.from_utc_datetime(&timestamp_to_naive_datetime(timestamp, time_unit))
}

/// Converts a [`chrono::DateTime`] into a timestamp in `time_unit`.
#[inline]
pub fn datetime_to_timestamp<T: chrono::TimeZone>(
    datetime: chrono::DateTime<T>,
    time_unit: TimeUnit,
) -> Result<i64, DaftError> {
    match time_unit {
        TimeUnit::Seconds => Ok(datetime.timestamp()),
        TimeUnit::Milliseconds => Ok(datetime.timestamp_millis()),
        TimeUnit::Microseconds => Ok(datetime.timestamp_micros()),
        TimeUnit::Nanoseconds => datetime.timestamp_nanos_opt().ok_or_else(|| {
            DaftError::ValueError(
                "Error converting timestamp to nanoseconds; datetime out of bounds".to_string(),
            )
        }),
    }
}

/// Converts a naive datetime (interpreted as UTC) into a timestamp in `time_unit`.
#[inline]
pub fn naive_datetime_to_timestamp(
    naive: chrono::NaiveDateTime,
    time_unit: TimeUnit,
) -> Result<i64, DaftError> {
    datetime_to_timestamp(naive.and_utc(), time_unit)
}

/// Parses an offset of the form `"+WX:YZ"` or `"UTC"` into [`FixedOffset`].
/// # Errors
/// If the offset is not in any of the allowed forms.
pub fn parse_offset(offset: &str) -> Result<chrono::FixedOffset, Error> {
    if offset == "UTC" {
        return Ok(chrono::FixedOffset::east_opt(0).expect("FixedOffset::east out of bounds"));
    }
    let error = "timezone offset must be of the form [-]00:00";

    let mut a = offset.split(':');
    let first = a
        .next()
        .map(Ok)
        .unwrap_or_else(|| Err(Error::InvalidArgumentError(error.to_string())))?;
    let last = a
        .next()
        .map(Ok)
        .unwrap_or_else(|| Err(Error::InvalidArgumentError(error.to_string())))?;
    let hours: i32 = first
        .parse()
        .map_err(|_| Error::InvalidArgumentError(error.to_string()))?;
    let minutes: i32 = last
        .parse()
        .map_err(|_| Error::InvalidArgumentError(error.to_string()))?;

    Ok(
        chrono::FixedOffset::east_opt(hours * 60 * 60 + minutes * 60)
            .expect("FixedOffset::east out of bounds"),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParsedTimezone {
    Fixed(chrono::FixedOffset),
    Tz(chrono_tz::Tz),
}

impl Display for ParsedTimezone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fixed(offset) => write!(f, "{offset}"),
            Self::Tz(tz) => write!(f, "{tz}"),
        }
    }
}

impl ParsedTimezone {
    /// Converts a UTC [`NaiveDateTime`] to a [`DateTime`] with a fixed offset in this timezone.
    #[inline]
    pub fn from_utc_datetime(
        &self,
        naive: &chrono::NaiveDateTime,
    ) -> chrono::DateTime<chrono::FixedOffset> {
        match self {
            Self::Fixed(offset) => offset.from_utc_datetime(naive).fixed_offset(),
            Self::Tz(tz) => tz.from_utc_datetime(naive).fixed_offset(),
        }
    }

    /// Converts a local [`NaiveDateTime`] to a [`DateTime`] with a fixed offset in this timezone.
    ///
    /// Returns `LocalResult` to handle ambiguous or nonexistent local times (e.g., during DST transitions).
    #[inline]
    pub fn from_local_datetime(
        &self,
        naive: &chrono::NaiveDateTime,
    ) -> LocalResult<chrono::DateTime<chrono::FixedOffset>> {
        match self {
            Self::Fixed(offset) => offset
                .from_local_datetime(naive)
                .map(|dt| dt.fixed_offset()),
            Self::Tz(tz) => tz.from_local_datetime(naive).map(|dt| dt.fixed_offset()),
        }
    }
}

/// Parses either a fixed offset or tz database name into [`ParsedTimezone`].
pub fn parse_timezone(tz: &str) -> Result<ParsedTimezone, Error> {
    if let Ok(offset) = parse_offset(tz) {
        Ok(ParsedTimezone::Fixed(offset))
    } else if let Ok(tz) = tz.parse::<chrono_tz::Tz>() {
        Ok(ParsedTimezone::Tz(tz))
    } else {
        Err(Error::InvalidArgumentError(format!(
            "Unable to parse timezone string {tz}"
        )))
    }
}

/// Resolves a naive local datetime in `tz` into a timestamp in `timeunit`.
pub fn naive_local_to_timestamp(
    naive: chrono::NaiveDateTime,
    timeunit: TimeUnit,
    tz: &ParsedTimezone,
    tz_name: &str,
) -> Result<i64, DaftError> {
    let datetime = match tz.from_local_datetime(&naive) {
        LocalResult::Single(dt) => dt,
        LocalResult::Ambiguous(_, _) => {
            return Err(DaftError::ValueError(format!(
                "Ambiguous local datetime {naive} in timezone {tz_name}"
            )));
        }
        LocalResult::None => {
            return Err(DaftError::ValueError(format!(
                "Nonexistent local datetime {naive} in timezone {tz_name}"
            )));
        }
    };
    datetime_to_timestamp(datetime, timeunit)
}

/// Converts a timestamp to the naive local datetime in `tz`.
pub fn timestamp_to_naive_local(
    timestamp: i64,
    timeunit: TimeUnit,
    tz: &ParsedTimezone,
) -> chrono::NaiveDateTime {
    let naive_utc = timestamp_to_naive_datetime(timestamp, timeunit);
    tz.from_utc_datetime(&naive_utc).naive_local()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_string_has_offset() {
        let test_cases = vec![
            ("%Y-%m-%d %H:%M:%S %Z", true),
            ("%Y-%m-%d %H:%M:%S %z", true),
            ("%Y-%m-%d %H:%M:%S %:z", true),
            ("%Y-%m-%d %H:%M:%S %::z", true),
            ("%Y-%m-%d %H:%M:%S %#z", true),
            ("%+", true),
            ("Date: %Y-%m-%d Time: %H:%M:%S Zone: %Z", true),
            ("%Y-%m-%dT%H:%M:%S%z", true),
            ("%Y-%m-%d %H:%M:%S.%f %z", true),
            ("%c %Z", true),
            ("%Y-%m-%d %H:%M:%S", false),
            ("%Y-%m-%dT%H:%M:%S", false),
            ("%Y z %d", false),
            ("%Y-%m-%d", false),
            ("%H:%M:%S", false),
            ("%Y-%m-%d %H:%M:%S.%f", false),
            ("", false),
            ("random text", false),
            ("%%z", true),
        ];

        for (format, expected) in test_cases {
            assert_eq!(
                format_string_has_offset(format),
                expected,
                "Failed for format: {}",
                format
            );
        }
    }
}
