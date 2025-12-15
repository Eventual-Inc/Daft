use chrono::{Datelike, Timelike};
use csv_async;
use daft_arrow::{
    array::{
        Array, BinaryArray, BooleanArray, MutableBinaryArray, MutableUtf8Array, NullArray,
        PrimitiveArray, Utf8Array,
    },
    datatypes::{DataType, TimeUnit},
    error::{Error, Result},
    io::csv,
    offset::Offset,
    temporal_conversions,
    trusted_len::TrustedLen,
    types::NativeType,
};

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

impl ByteRecordGeneric for csv::read::ByteRecord {
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
fn deserialize_primitive<'a, T, I, F>(
    bytes_iter: I,
    datatype: DataType,
    mut op: F,
) -> Box<dyn Array>
where
    T: NativeType,
    I: TrustedLen<Item = Option<&'a [u8]>>,
    F: FnMut(&[u8]) -> Option<T>,
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
    Box::new(PrimitiveArray::<T>::from_trusted_len_iter(iter).to(datatype))
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
fn deserialize_boolean<'a, I, F>(bytes_iter: I, op: F) -> Box<dyn Array>
where
    I: TrustedLen<Item = Option<&'a [u8]>>,
    F: Fn(&[u8]) -> Option<bool>,
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
    Box::new(BooleanArray::from_trusted_len_iter(iter))
}

#[inline]
fn deserialize_utf8<'a, O: Offset, I>(
    bytes_iter: I,
    expected_capacity: usize,
    expected_size: usize,
) -> Box<dyn Array>
where
    I: TrustedLen<Item = Option<&'a [u8]>>,
{
    let iter = bytes_iter.map(|bytes_opt| match bytes_opt {
        Some(bytes) => to_utf8(bytes),
        None => None,
    });

    let mut mu = MutableUtf8Array::<O>::with_capacities(expected_capacity, expected_size);
    mu.extend_trusted_len(iter);
    let array: Utf8Array<O> = mu.into();
    Box::new(array)
}

#[inline]
fn deserialize_binary<'a, O: Offset, I>(
    bytes_iter: I,
    expected_capacity: usize,
    expected_size: usize,
) -> Box<dyn Array>
where
    I: TrustedLen<Item = Option<&'a [u8]>>,
{
    let mut mu = MutableBinaryArray::<O>::with_capacities(expected_capacity, expected_size);
    mu.extend_trusted_len(bytes_iter);
    let array: BinaryArray<O> = mu.into();
    Box::new(array)
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

/// Unified deserialization function that works with any iterator of byte slices
#[inline]
pub fn deserialize_bytes_to_array<'a, I>(
    bytes_iter: I,
    datatype: DataType,
    expected_capacity: usize,
    expected_size: usize,
) -> Result<Box<dyn Array>>
where
    I: TrustedLen<Item = Option<&'a [u8]>>,
{
    use DataType::{
        Binary, Boolean, Date32, Date64, Decimal, Float32, Float64, Int8, Int16, Int32, Int64,
        LargeBinary, LargeUtf8, Null, Time32, Time64, Timestamp, UInt8, UInt16, UInt32, UInt64,
        Utf8,
    };
    Ok(match datatype {
        Boolean => deserialize_boolean(bytes_iter, |bytes| {
            if bytes.eq_ignore_ascii_case(b"false") {
                Some(false)
            } else if bytes.eq_ignore_ascii_case(b"true") {
                Some(true)
            } else {
                None
            }
        }),
        Int8 => deserialize_primitive(bytes_iter, datatype, |bytes| {
            atoi_simd::parse_skipped::<i8>(bytes).ok()
        }),
        Int16 => deserialize_primitive(bytes_iter, datatype, |bytes| {
            atoi_simd::parse_skipped::<i16>(bytes).ok()
        }),
        Int32 => deserialize_primitive(bytes_iter, datatype, |bytes| {
            atoi_simd::parse_skipped::<i32>(bytes).ok()
        }),
        Int64 => deserialize_primitive(bytes_iter, datatype, |bytes| {
            atoi_simd::parse_skipped::<i64>(bytes).ok()
        }),
        UInt8 => deserialize_primitive(bytes_iter, datatype, |bytes| {
            atoi_simd::parse_skipped::<u8>(bytes).ok()
        }),
        UInt16 => deserialize_primitive(bytes_iter, datatype, |bytes| {
            atoi_simd::parse_skipped::<u16>(bytes).ok()
        }),
        UInt32 => deserialize_primitive(bytes_iter, datatype, |bytes| {
            atoi_simd::parse_skipped::<u32>(bytes).ok()
        }),
        UInt64 => deserialize_primitive(bytes_iter, datatype, |bytes| {
            atoi_simd::parse_skipped::<u64>(bytes).ok()
        }),
        Float32 => deserialize_primitive(bytes_iter, datatype, |bytes| {
            fast_float2::parse::<f32, _>(bytes).ok()
        }),
        Float64 => deserialize_primitive(bytes_iter, datatype, |bytes| {
            fast_float2::parse::<f64, _>(bytes).ok()
        }),
        Date32 => deserialize_primitive(bytes_iter, datatype, |bytes| {
            let mut last_fmt_idx = 0;
            to_utf8(bytes)
                .and_then(|x| deserialize_naive_date(x, &mut last_fmt_idx))
                .map(|x| x.num_days_from_ce() - (temporal_conversions::UNIX_EPOCH_DAY as i32))
        }),
        Date64 => deserialize_primitive(bytes_iter, datatype, |bytes| {
            let mut last_fmt_idx = 0;
            to_utf8(bytes)
                .and_then(|x| deserialize_naive_datetime(x, &mut last_fmt_idx))
                .map(|x| x.and_utc().timestamp_millis())
        }),
        Time32(time_unit) => deserialize_primitive(bytes_iter, datatype, |bytes| {
            let factor = get_factor_from_timeunit(time_unit);
            to_utf8(bytes)
                .and_then(|x| x.parse::<chrono::NaiveTime>().ok())
                .map(|x| {
                    (x.hour() * 3_600 * factor
                        + x.minute() * 60 * factor
                        + x.second() * factor
                        + x.nanosecond() / (1_000_000_000 / factor)) as i32
                })
        }),
        Time64(time_unit) => deserialize_primitive(bytes_iter, datatype, |bytes| {
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
        }),
        Timestamp(time_unit, None) => {
            let mut last_fmt_idx = 0;
            deserialize_primitive(bytes_iter, datatype, |bytes| {
                to_utf8(bytes)
                    .and_then(|s| deserialize_naive_datetime(s, &mut last_fmt_idx))
                    .and_then(|dt| match time_unit {
                        TimeUnit::Second => Some(dt.and_utc().timestamp()),
                        TimeUnit::Millisecond => Some(dt.and_utc().timestamp_millis()),
                        TimeUnit::Microsecond => Some(dt.and_utc().timestamp_micros()),
                        TimeUnit::Nanosecond => dt.and_utc().timestamp_nanos_opt(),
                    })
            })
        }
        Timestamp(time_unit, Some(ref tz)) => {
            let tz = daft_schema::time_unit::parse_offset(tz)?;
            let mut last_fmt_idx = 0;
            deserialize_primitive(bytes_iter, datatype, |bytes| {
                to_utf8(bytes)
                    .and_then(|x| deserialize_datetime(x, &tz, &mut last_fmt_idx))
                    .and_then(|dt| match time_unit {
                        TimeUnit::Second => Some(dt.timestamp()),
                        TimeUnit::Millisecond => Some(dt.timestamp_millis()),
                        TimeUnit::Microsecond => Some(dt.timestamp_micros()),
                        TimeUnit::Nanosecond => dt.timestamp_nanos_opt(),
                    })
            })
        }
        Decimal(precision, scale) => deserialize_primitive(bytes_iter, datatype, |x| {
            deserialize_decimal(x, precision, scale)
        }),
        Utf8 => deserialize_utf8::<i32, _>(bytes_iter, expected_capacity, expected_size),
        LargeUtf8 => deserialize_utf8::<i64, _>(bytes_iter, expected_capacity, expected_size),
        Binary => deserialize_binary::<i32, _>(bytes_iter, expected_capacity, expected_size),
        LargeBinary => deserialize_binary::<i64, _>(bytes_iter, expected_capacity, expected_size),
        Null => Box::new(NullArray::new(DataType::Null, expected_capacity)),
        other => {
            return Err(Error::NotYetImplemented(format!(
                "Deserializing type \"{other:?}\" is not implemented"
            )));
        }
    })
}

/// Deserializes `column` of CSV byte records into an [`Array`] of [`DataType`] `datatype`.
#[inline]
pub fn deserialize_column<B: ByteRecordGeneric>(
    rows: &[B],
    column: usize,
    datatype: DataType,
    _line_number: usize,
) -> Result<Box<dyn Array>> {
    let bytes_iter = rows.iter().map(|row| row.get(column));
    let expected_capacity = rows.len();
    let expected_size = rows
        .iter()
        .map(|row| row.get(column).map(|bytes| bytes.len()).unwrap_or(0))
        .sum::<usize>();
    deserialize_bytes_to_array(bytes_iter, datatype, expected_capacity, expected_size)
}

/// Deserializes a single value from bytes to a single-element Arrow2 array of the specified DataType.
pub fn deserialize_single_value_to_arrow(
    bytes: &[u8],
    datatype: DataType,
) -> Result<Box<dyn Array>> {
    let bytes_iter = std::iter::once(if bytes.is_empty() { None } else { Some(bytes) });
    deserialize_bytes_to_array(bytes_iter, datatype, 1, bytes.len())
}
