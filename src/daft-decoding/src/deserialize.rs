use std::sync::Arc;

use arrow_array::{
    ArrayRef, ArrowPrimitiveType, PrimitiveArray,
    builder::{
        BinaryBuilder, BooleanBuilder, LargeBinaryBuilder, LargeStringBuilder, StringBuilder,
    },
    new_null_array,
};
use arrow_schema::{ArrowError, DataType, TimeUnit};
use chrono::{Datelike, Timelike};

pub(crate) const ISO8601: &str = "%+";
pub(crate) const ISO8601_NO_TIME_ZONE: &str = "%Y-%m-%dT%H:%M:%S%.f";
pub(crate) const ISO8601_NO_TIME_ZONE_NO_FRACTIONAL: &str = "%Y-%m-%dT%H:%M:%S";
pub(crate) const ISO8601_DATE: &str = "%Y-%m-%d";
pub(crate) const ISO8601_DATE_SLASHES: &str = "%Y/%m/%d";
pub(crate) const RFC3339_WITH_SPACE: &str = "%Y-%m-%d %H:%M:%S%.f%:z";
pub(crate) const RFC3339_WITH_SPACE_NO_TIME_ZONE: &str = "%Y-%m-%d %H:%M:%S%.f";
pub(crate) const RFC3339_WITH_SPACE_NO_TIME_ZONE_NO_FRACTIONAL: &str = "%Y-%m-%d %H:%M:%S";
pub(crate) const ALL_NAIVE_TIMESTAMP_FMTS: &[&str] = &[
    ISO8601_NO_TIME_ZONE,
    ISO8601_NO_TIME_ZONE_NO_FRACTIONAL,
    RFC3339_WITH_SPACE_NO_TIME_ZONE,
    RFC3339_WITH_SPACE_NO_TIME_ZONE_NO_FRACTIONAL,
    ISO8601_DATE,
    ISO8601_DATE_SLASHES,
];
pub(crate) const ALL_TIMESTAMP_FMTS: &[&str] = &[ISO8601, RFC3339_WITH_SPACE];
pub(crate) const ALL_NAIVE_DATE_FMTS: &[&str] = &[ISO8601_DATE, ISO8601_DATE_SLASHES];

/// Number of days between 0001-01-01 and 1970-01-01 (the Unix epoch).
const UNIX_EPOCH_DAY: i32 = 719_163;

// Ideally this trait should not be needed and both `csv` and `csv_async` crates would share
// the same `ByteRecord` struct. Unfortunately, they do not and thus we must use generics
// over this trait and materialize the generics for each struct.
pub trait ByteRecordGeneric {
    fn get(&self, index: usize) -> Option<&[u8]>;
}

impl ByteRecordGeneric for csv_async::ByteRecord {
    #[inline]
    fn get(&self, index: usize) -> Option<&[u8]> {
        self.get(index)
    }
}

impl ByteRecordGeneric for csv::ByteRecord {
    #[inline]
    fn get(&self, index: usize) -> Option<&[u8]> {
        self.get(index)
    }
}

#[inline]
fn to_utf8(bytes: &[u8]) -> Option<&str> {
    simdutf8::basic::from_utf8(bytes).ok()
}

#[inline]
fn deserialize_primitive<'a, T, I, F>(bytes_iter: I, mut op: F) -> ArrayRef
where
    T: ArrowPrimitiveType,
    I: Iterator<Item = Option<&'a [u8]>>,
    F: FnMut(&[u8]) -> Option<T::Native>,
{
    let iter = bytes_iter.map(|bytes_opt| match bytes_opt {
        Some(bytes) => {
            if bytes.is_empty() {
                return None;
            }
            op(bytes)
        }
        None => None,
    });
    Arc::new(PrimitiveArray::<T>::from_iter(iter))
}

#[inline]
fn significant_bytes(bytes: &[u8]) -> usize {
    bytes.iter().map(|byte| usize::from(*byte != b'0')).sum()
}

/// Deserializes bytes to a single i128 representing a decimal
/// The decimal precision and scale are not checked.
#[inline]
fn deserialize_decimal(bytes: &[u8], precision: usize, scale: usize) -> Option<i128> {
    let mut a = bytes.split(|x| *x == b'.');
    let lhs = a.next();
    let rhs = a.next();
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => atoi_simd::parse_skipped::<i128>(lhs).ok().and_then(|x| {
            atoi_simd::parse_skipped::<i128>(rhs)
                .ok()
                .map(|y| (x, lhs, y, rhs))
                .and_then(|(lhs, lhs_b, rhs, rhs_b)| {
                    let lhs_s = significant_bytes(lhs_b);
                    let rhs_s = significant_bytes(rhs_b);
                    if lhs_s + rhs_s > precision || rhs_s > scale {
                        None
                    } else {
                        Some((lhs, rhs, rhs_s))
                    }
                })
                .map(|(lhs, rhs, rhs_s)| lhs * 10i128.pow(rhs_s as u32) + rhs)
        }),
        (None, Some(rhs)) => {
            if rhs.len() != precision || rhs.len() != scale {
                return None;
            }
            atoi_simd::parse_skipped::<i128>(rhs).ok()
        }
        (Some(lhs), None) => {
            if lhs.len() != precision || scale != 0 {
                return None;
            }
            atoi_simd::parse_skipped::<i128>(lhs).ok()
        }
        (None, None) => None,
    }
}

#[inline]
fn deserialize_boolean<'a, I, F>(bytes_iter: I, expected_capacity: usize, op: F) -> ArrayRef
where
    I: Iterator<Item = Option<&'a [u8]>>,
    F: Fn(&[u8]) -> Option<bool>,
{
    let mut builder = BooleanBuilder::with_capacity(expected_capacity);
    for bytes_opt in bytes_iter {
        match bytes_opt {
            Some(bytes) => {
                if bytes.is_empty() {
                    builder.append_null();
                } else {
                    match op(bytes) {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
            }
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

#[inline]
fn deserialize_utf8<'a, I>(bytes_iter: I, expected_capacity: usize) -> ArrayRef
where
    I: Iterator<Item = Option<&'a [u8]>>,
{
    let mut builder = StringBuilder::with_capacity(expected_capacity, expected_capacity * 8);
    for bytes_opt in bytes_iter {
        match bytes_opt {
            Some(bytes) => match to_utf8(bytes) {
                Some(s) => builder.append_value(s),
                None => builder.append_null(),
            },
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

#[inline]
fn deserialize_large_utf8<'a, I>(bytes_iter: I, expected_capacity: usize) -> ArrayRef
where
    I: Iterator<Item = Option<&'a [u8]>>,
{
    let mut builder = LargeStringBuilder::with_capacity(expected_capacity, expected_capacity * 8);
    for bytes_opt in bytes_iter {
        match bytes_opt {
            Some(bytes) => match to_utf8(bytes) {
                Some(s) => builder.append_value(s),
                None => builder.append_null(),
            },
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

#[inline]
fn deserialize_binary<'a, I>(bytes_iter: I, expected_capacity: usize) -> ArrayRef
where
    I: Iterator<Item = Option<&'a [u8]>>,
{
    let mut builder = BinaryBuilder::with_capacity(expected_capacity, expected_capacity * 8);
    for bytes_opt in bytes_iter {
        match bytes_opt {
            Some(bytes) => builder.append_value(bytes),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

#[inline]
fn deserialize_large_binary<'a, I>(bytes_iter: I, expected_capacity: usize) -> ArrayRef
where
    I: Iterator<Item = Option<&'a [u8]>>,
{
    let mut builder = LargeBinaryBuilder::with_capacity(expected_capacity, expected_capacity * 8);
    for bytes_opt in bytes_iter {
        match bytes_opt {
            Some(bytes) => builder.append_value(bytes),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

// Return the factor by how small is a time unit compared to seconds
#[must_use]
pub fn get_factor_from_timeunit(time_unit: TimeUnit) -> u32 {
    match time_unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

#[inline]
pub fn deserialize_naive_date(string: &str, fmt_idx: &mut usize) -> Option<chrono::NaiveDate> {
    // TODO(Clark): Parse as all candidate formats in a single pass.
    for i in 0..ALL_NAIVE_DATE_FMTS.len() {
        let idx = (i + *fmt_idx) % ALL_NAIVE_DATE_FMTS.len();
        let fmt = ALL_NAIVE_DATE_FMTS[idx];
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(string, fmt) {
            *fmt_idx = idx;
            return Some(dt);
        }
    }
    None
}

#[inline]
pub fn deserialize_naive_datetime(
    string: &str,
    fmt_idx: &mut usize,
) -> Option<chrono::NaiveDateTime> {
    // TODO(Clark): Parse as all candidate formats in a single pass.
    for i in 0..ALL_NAIVE_TIMESTAMP_FMTS.len() {
        let idx = (i + *fmt_idx) % ALL_NAIVE_TIMESTAMP_FMTS.len();
        let fmt = ALL_NAIVE_TIMESTAMP_FMTS[idx];
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(string, fmt) {
            *fmt_idx = idx;
            return Some(dt);
        }
    }
    None
}

#[inline]
pub fn deserialize_datetime<T: chrono::TimeZone>(
    string: &str,
    tz: &T,
    fmt_idx: &mut usize,
) -> Option<chrono::DateTime<T>> {
    // TODO(Clark): Parse as all candidate formats in a single pass.
    for i in 0..ALL_TIMESTAMP_FMTS.len() {
        let idx = (i + *fmt_idx) % ALL_TIMESTAMP_FMTS.len();
        let fmt = ALL_TIMESTAMP_FMTS[idx];
        if let Ok(dt) = chrono::DateTime::parse_from_str(string, fmt) {
            *fmt_idx = idx;
            return Some(dt.with_timezone(tz));
        }
    }
    None
}

/// Unified deserialization function that works with any iterator of byte slices.
#[inline]
pub fn deserialize_bytes_to_array<'a, I>(
    bytes_iter: I,
    datatype: DataType,
    expected_capacity: usize,
    _expected_size: usize,
) -> Result<ArrayRef, ArrowError>
where
    I: Iterator<Item = Option<&'a [u8]>>,
{
    use DataType::*;
    use arrow_array::types::*;
    Ok(match datatype {
        Boolean => deserialize_boolean(bytes_iter, expected_capacity, |bytes| {
            if bytes.eq_ignore_ascii_case(b"false") {
                Some(false)
            } else if bytes.eq_ignore_ascii_case(b"true") {
                Some(true)
            } else {
                None
            }
        }),
        Int8 => deserialize_primitive::<Int8Type, _, _>(bytes_iter, |bytes| {
            atoi_simd::parse_skipped::<i8>(bytes).ok()
        }),
        Int16 => deserialize_primitive::<Int16Type, _, _>(bytes_iter, |bytes| {
            atoi_simd::parse_skipped::<i16>(bytes).ok()
        }),
        Int32 => deserialize_primitive::<Int32Type, _, _>(bytes_iter, |bytes| {
            atoi_simd::parse_skipped::<i32>(bytes).ok()
        }),
        Int64 => deserialize_primitive::<Int64Type, _, _>(bytes_iter, |bytes| {
            atoi_simd::parse_skipped::<i64>(bytes).ok()
        }),
        UInt8 => deserialize_primitive::<UInt8Type, _, _>(bytes_iter, |bytes| {
            atoi_simd::parse_skipped::<u8>(bytes).ok()
        }),
        UInt16 => deserialize_primitive::<UInt16Type, _, _>(bytes_iter, |bytes| {
            atoi_simd::parse_skipped::<u16>(bytes).ok()
        }),
        UInt32 => deserialize_primitive::<UInt32Type, _, _>(bytes_iter, |bytes| {
            atoi_simd::parse_skipped::<u32>(bytes).ok()
        }),
        UInt64 => deserialize_primitive::<UInt64Type, _, _>(bytes_iter, |bytes| {
            atoi_simd::parse_skipped::<u64>(bytes).ok()
        }),
        Float32 => deserialize_primitive::<Float32Type, _, _>(bytes_iter, |bytes| {
            fast_float2::parse::<f32, _>(bytes).ok()
        }),
        Float64 => deserialize_primitive::<Float64Type, _, _>(bytes_iter, |bytes| {
            fast_float2::parse::<f64, _>(bytes).ok()
        }),
        Date32 => deserialize_primitive::<Date32Type, _, _>(bytes_iter, |bytes| {
            let mut last_fmt_idx = 0;
            to_utf8(bytes)
                .and_then(|x| deserialize_naive_date(x, &mut last_fmt_idx))
                .map(|x| x.num_days_from_ce() - UNIX_EPOCH_DAY)
        }),
        Date64 => deserialize_primitive::<Date64Type, _, _>(bytes_iter, |bytes| {
            let mut last_fmt_idx = 0;
            to_utf8(bytes)
                .and_then(|x| deserialize_naive_datetime(x, &mut last_fmt_idx))
                .map(|x| x.and_utc().timestamp_millis())
        }),
        Time32(time_unit) => {
            let parse_time = |bytes: &[u8]| {
                let factor = get_factor_from_timeunit(time_unit);
                to_utf8(bytes)
                    .and_then(|x| x.parse::<chrono::NaiveTime>().ok())
                    .map(|x| {
                        (x.hour() * 3_600 * factor
                            + x.minute() * 60 * factor
                            + x.second() * factor
                            + x.nanosecond() / (1_000_000_000 / factor))
                            as i32
                    })
            };
            match time_unit {
                TimeUnit::Second => {
                    deserialize_primitive::<Time32SecondType, _, _>(bytes_iter, parse_time)
                }
                TimeUnit::Millisecond => {
                    deserialize_primitive::<Time32MillisecondType, _, _>(bytes_iter, parse_time)
                }
                _ => {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Time32 does not support {time_unit:?}"
                    )));
                }
            }
        }
        Time64(time_unit) => {
            let parse_time = |bytes: &[u8]| {
                let factor: u64 = get_factor_from_timeunit(time_unit).into();
                to_utf8(bytes)
                    .and_then(|x| x.parse::<chrono::NaiveTime>().ok())
                    .map(|x| {
                        (u64::from(x.hour()) * 3_600 * factor
                            + u64::from(x.minute()) * 60 * factor
                            + u64::from(x.second()) * factor
                            + u64::from(x.nanosecond()) / (1_000_000_000 / factor))
                            as i64
                    })
            };
            match time_unit {
                TimeUnit::Microsecond => {
                    deserialize_primitive::<Time64MicrosecondType, _, _>(bytes_iter, parse_time)
                }
                TimeUnit::Nanosecond => {
                    deserialize_primitive::<Time64NanosecondType, _, _>(bytes_iter, parse_time)
                }
                _ => {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Time64 does not support {time_unit:?}"
                    )));
                }
            }
        }
        Timestamp(time_unit, None) => {
            let mut last_fmt_idx = 0;
            match time_unit {
                TimeUnit::Second => {
                    deserialize_primitive::<TimestampSecondType, _, _>(bytes_iter, |bytes| {
                        to_utf8(bytes)
                            .and_then(|s| deserialize_naive_datetime(s, &mut last_fmt_idx))
                            .map(|dt| dt.and_utc().timestamp())
                    })
                }
                TimeUnit::Millisecond => {
                    deserialize_primitive::<TimestampMillisecondType, _, _>(bytes_iter, |bytes| {
                        to_utf8(bytes)
                            .and_then(|s| deserialize_naive_datetime(s, &mut last_fmt_idx))
                            .map(|dt| dt.and_utc().timestamp_millis())
                    })
                }
                TimeUnit::Microsecond => {
                    deserialize_primitive::<TimestampMicrosecondType, _, _>(bytes_iter, |bytes| {
                        to_utf8(bytes)
                            .and_then(|s| deserialize_naive_datetime(s, &mut last_fmt_idx))
                            .map(|dt| dt.and_utc().timestamp_micros())
                    })
                }
                TimeUnit::Nanosecond => {
                    deserialize_primitive::<TimestampNanosecondType, _, _>(bytes_iter, |bytes| {
                        to_utf8(bytes)
                            .and_then(|s| deserialize_naive_datetime(s, &mut last_fmt_idx))
                            .and_then(|dt| dt.and_utc().timestamp_nanos_opt())
                    })
                }
            }
        }
        Timestamp(time_unit, Some(ref tz)) => {
            let tz = daft_schema::time_unit::parse_offset(tz).map_err(|e| {
                ArrowError::InvalidArgumentError(format!("Failed to parse timezone: {e}"))
            })?;
            let mut last_fmt_idx = 0;
            match time_unit {
                TimeUnit::Second => {
                    deserialize_primitive::<TimestampSecondType, _, _>(bytes_iter, |bytes| {
                        to_utf8(bytes)
                            .and_then(|x| deserialize_datetime(x, &tz, &mut last_fmt_idx))
                            .map(|dt| dt.timestamp())
                    })
                }
                TimeUnit::Millisecond => {
                    deserialize_primitive::<TimestampMillisecondType, _, _>(bytes_iter, |bytes| {
                        to_utf8(bytes)
                            .and_then(|x| deserialize_datetime(x, &tz, &mut last_fmt_idx))
                            .map(|dt| dt.timestamp_millis())
                    })
                }
                TimeUnit::Microsecond => {
                    deserialize_primitive::<TimestampMicrosecondType, _, _>(bytes_iter, |bytes| {
                        to_utf8(bytes)
                            .and_then(|x| deserialize_datetime(x, &tz, &mut last_fmt_idx))
                            .map(|dt| dt.timestamp_micros())
                    })
                }
                TimeUnit::Nanosecond => {
                    deserialize_primitive::<TimestampNanosecondType, _, _>(bytes_iter, |bytes| {
                        to_utf8(bytes)
                            .and_then(|x| deserialize_datetime(x, &tz, &mut last_fmt_idx))
                            .and_then(|dt| dt.timestamp_nanos_opt())
                    })
                }
            }
        }
        Decimal128(precision, scale) => {
            deserialize_primitive::<Decimal128Type, _, _>(bytes_iter, |x| {
                deserialize_decimal(x, precision as usize, scale as usize)
            })
        }
        Utf8 => deserialize_utf8(bytes_iter, expected_capacity),
        LargeUtf8 => deserialize_large_utf8(bytes_iter, expected_capacity),
        Binary => deserialize_binary(bytes_iter, expected_capacity),
        LargeBinary => deserialize_large_binary(bytes_iter, expected_capacity),
        Null => new_null_array(&DataType::Null, expected_capacity),
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "Deserializing type \"{other:?}\" is not implemented"
            )));
        }
    })
}

/// Deserializes `column` of CSV byte records into an [`ArrayRef`] of the given [`DataType`].
#[inline]
pub fn deserialize_column<B: ByteRecordGeneric>(
    rows: &[B],
    column: usize,
    datatype: DataType,
    _line_number: usize,
) -> Result<ArrayRef, ArrowError> {
    let bytes_iter = rows.iter().map(|row| row.get(column));
    let expected_capacity = rows.len();
    let expected_size = rows
        .iter()
        .map(|row| row.get(column).map(|bytes| bytes.len()).unwrap_or(0))
        .sum::<usize>();
    deserialize_bytes_to_array(bytes_iter, datatype, expected_capacity, expected_size)
}

/// Deserializes a single value from bytes to a single-element arrow-rs array of the specified DataType.
pub fn deserialize_single_value_to_arrow(
    bytes: &[u8],
    datatype: DataType,
) -> Result<ArrayRef, ArrowError> {
    let bytes_iter = std::iter::once(if bytes.is_empty() { None } else { Some(bytes) });
    deserialize_bytes_to_array(bytes_iter, datatype, 1, bytes.len())
}
