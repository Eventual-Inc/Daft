use arrow2::datatypes::TimeUnit;
use chrono::Timelike;

use crate::deserialize::{ALL_NAIVE_TIMESTAMP_FMTS, ALL_TIMESTAMP_FMTS};

/// Infers [`DataType`] from `bytes`
/// # Implementation
/// * empty slice to [`DataType::Null`]
/// * case insensitive "true" or "false" are mapped to [`DataType::Boolean`]
/// * parsable to integer is mapped to [`DataType::Int64`]
/// * parsable to float is mapped to [`DataType::Float64`]
/// * parsable to date is mapped to [`DataType::Date32`]
/// * parsable to time is mapped to [`DataType::Time32(TimeUnit::Millisecond)`]
/// * parsable to naive datetime is mapped to [`DataType::Timestamp(TimeUnit::Millisecond, None)`]
/// * parsable to time-aware datetime is mapped to [`DataType::Timestamp`] of milliseconds and parsed offset.
/// * other utf8 is mapped to [`DataType::Utf8`]
/// * invalid utf8 is mapped to [`DataType::Binary`]
pub fn infer(bytes: &[u8]) -> arrow2::datatypes::DataType {
    use arrow2::datatypes::DataType;
    if is_null(bytes) {
        DataType::Null
    } else if is_boolean(bytes) {
        DataType::Boolean
    } else if is_integer(bytes) {
        DataType::Int64
    } else if is_float(bytes) {
        DataType::Float64
    } else if let Ok(string) = simdutf8::basic::from_utf8(bytes) {
        if is_date(string) {
            DataType::Date32
        } else if is_time(string) {
            DataType::Time32(TimeUnit::Millisecond)
        } else if let Some(time_unit) = is_naive_datetime(string) {
            DataType::Timestamp(time_unit, None)
        } else if let Some((time_unit, offset)) = is_datetime(string) {
            DataType::Timestamp(time_unit, Some(offset))
        } else {
            DataType::Utf8
        }
    } else {
        // invalid utf8
        DataType::Binary
    }
}

fn is_null(bytes: &[u8]) -> bool {
    bytes.is_empty()
}

fn is_boolean(bytes: &[u8]) -> bool {
    bytes.eq_ignore_ascii_case(b"true") | bytes.eq_ignore_ascii_case(b"false")
}

fn is_float(bytes: &[u8]) -> bool {
    lexical_core::parse::<f64>(bytes).is_ok()
}

fn is_integer(bytes: &[u8]) -> bool {
    lexical_core::parse::<i64>(bytes).is_ok()
}

fn is_date(string: &str) -> bool {
    string.parse::<chrono::NaiveDate>().is_ok()
}

fn is_time(string: &str) -> bool {
    string.parse::<chrono::NaiveTime>().is_ok()
}

fn is_naive_datetime(string: &str) -> Option<TimeUnit> {
    for fmt in ALL_NAIVE_TIMESTAMP_FMTS {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(string, fmt) {
            let time_unit = nanoseconds_to_time_unit(dt.nanosecond());
            return Some(time_unit);
        }
    }
    None
}

fn is_datetime(string: &str) -> Option<(TimeUnit, String)> {
    for fmt in ALL_TIMESTAMP_FMTS {
        if let Ok(dt) = chrono::DateTime::parse_from_str(string, fmt) {
            let offset = dt.offset().local_minus_utc();
            let hours = offset / 60 / 60;
            let minutes = offset / 60 - hours * 60;
            let time_unit = nanoseconds_to_time_unit(dt.nanosecond());
            return Some((time_unit, format!("{hours:+03}:{minutes:02}")));
        }
    }
    None
}

fn nanoseconds_to_time_unit(ns: u32) -> TimeUnit {
    if ns % 1_000 != 0 {
        TimeUnit::Nanosecond
    } else if ns % 1_000_000 != 0 {
        TimeUnit::Microsecond
    } else if ns % 1_000_000_000 != 0 {
        TimeUnit::Millisecond
    } else {
        TimeUnit::Second
    }
}
