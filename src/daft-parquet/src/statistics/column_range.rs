use std::sync::Arc;

use daft_core::prelude::*;
use daft_stats::ColumnRangeStatistics;
use snafu::ResultExt;

use super::{
    DaftStatsSnafu, UnableToParseUtf8FromBinarySnafu,
    utils::{convert_i96_to_i64_timestamp, convert_i128, timeunit_to_daft},
};

fn make_decimal_column_range_statistics(
    p: usize,
    s: usize,
    lower: &[u8],
    upper: &[u8],
) -> super::Result<ColumnRangeStatistics> {
    if lower.len() > 16 || upper.len() > 16 {
        return Ok(ColumnRangeStatistics::Missing);
    }
    let l = convert_i128(lower, lower.len());
    let u = convert_i128(upper, upper.len());

    let daft_type = daft_core::datatypes::DataType::Decimal128(p, s);
    let lower_field = Arc::new(daft_core::datatypes::Field::new("lower", daft_type.clone()));
    let upper_field = Arc::new(daft_core::datatypes::Field::new("upper", daft_type));

    let lower = Decimal128Array::from_iter(lower_field, std::iter::once(Some(l))).into_series();
    let upper = Decimal128Array::from_iter(upper_field, std::iter::once(Some(u))).into_series();

    Ok(ColumnRangeStatistics::new(Some(lower), Some(upper))?)
}

fn make_date_column_range_statistics(
    lower: i32,
    upper: i32,
) -> super::Result<ColumnRangeStatistics> {
    let lower = Int32Array::from_slice("lower", &[lower]);
    let upper = Int32Array::from_slice("upper", &[upper]);

    let dtype = daft_core::datatypes::DataType::Date;

    let lower = DateArray::new(
        daft_core::datatypes::Field::new("lower", dtype.clone()),
        lower,
    )
    .into_series();
    let upper =
        DateArray::new(daft_core::datatypes::Field::new("upper", dtype), upper).into_series();
    Ok(ColumnRangeStatistics::new(Some(lower), Some(upper))?)
}

fn make_timestamp_column_range_statistics(
    tu: daft_core::datatypes::TimeUnit,
    is_adjusted_to_utc: bool,
    lower: i64,
    upper: i64,
) -> super::Result<ColumnRangeStatistics> {
    let lower_arr = Int64Array::from_slice("lower", &[lower]);
    let upper_arr = Int64Array::from_slice("upper", &[upper]);
    let tz = if is_adjusted_to_utc {
        Some("+00:00".to_string())
    } else {
        None
    };
    let dtype = daft_core::datatypes::DataType::Timestamp(tu, tz);
    let lower = TimestampArray::new(
        daft_core::datatypes::Field::new("lower", dtype.clone()),
        lower_arr,
    )
    .into_series();
    let upper = TimestampArray::new(daft_core::datatypes::Field::new("upper", dtype), upper_arr)
        .into_series();
    Ok(ColumnRangeStatistics::new(Some(lower), Some(upper))?)
}

// ============================================================================
// Arrow-rs parquet statistics conversion
// ============================================================================

use parquet::{
    basic::{ConvertedType as ArrowConvertedType, LogicalType as ArrowLogicalType},
    data_type::{ByteArray as ArrowByteArray, Int96 as ArrowInt96},
    file::statistics::Statistics as ArrowStatistics,
    schema::types::ColumnDescriptor as ArrowColumnDescriptor,
};

/// Converts arrow-rs parquet statistics to ColumnRangeStatistics.
pub fn parquet_statistics_to_column_range_statistics(
    stats: &ArrowStatistics,
    col_descr: &ArrowColumnDescriptor,
    daft_dtype: &DataType,
) -> Result<ColumnRangeStatistics, super::Error> {
    let daft_stats = match stats {
        ArrowStatistics::Boolean(s) => {
            let (Some(&lower), Some(&upper)) = (s.min_opt(), s.max_opt()) else {
                return Ok(ColumnRangeStatistics::Missing);
            };
            ColumnRangeStatistics::new(
                Some(BooleanArray::from_slice("lower", &[lower]).into_series()),
                Some(BooleanArray::from_slice("upper", &[upper]).into_series()),
            )?
        }
        ArrowStatistics::Int32(s) => {
            let (Some(&lower), Some(&upper)) = (s.min_opt(), s.max_opt()) else {
                return Ok(ColumnRangeStatistics::Missing);
            };
            convert_int32_arrowrs(lower, upper, col_descr)?
        }
        ArrowStatistics::Int64(s) => {
            let (Some(&lower), Some(&upper)) = (s.min_opt(), s.max_opt()) else {
                return Ok(ColumnRangeStatistics::Missing);
            };
            convert_int64_arrowrs(lower, upper, col_descr)?
        }
        ArrowStatistics::Int96(s) => {
            let (Some(lower), Some(upper)) = (s.min_opt(), s.max_opt()) else {
                return Ok(ColumnRangeStatistics::Missing);
            };
            convert_int96_arrowrs(lower, upper, col_descr)?
        }
        ArrowStatistics::Float(s) => {
            let (Some(&lower), Some(&upper)) = (s.min_opt(), s.max_opt()) else {
                return Ok(ColumnRangeStatistics::Missing);
            };
            let lower = Float32Array::from_slice("lower", &[lower]).into_series();
            let upper = Float32Array::from_slice("upper", &[upper]).into_series();
            ColumnRangeStatistics::new(Some(lower), Some(upper))?
        }
        ArrowStatistics::Double(s) => {
            let (Some(&lower), Some(&upper)) = (s.min_opt(), s.max_opt()) else {
                return Ok(ColumnRangeStatistics::Missing);
            };
            let lower = Float64Array::from_slice("lower", &[lower]).into_series();
            let upper = Float64Array::from_slice("upper", &[upper]).into_series();
            ColumnRangeStatistics::new(Some(lower), Some(upper))?
        }
        ArrowStatistics::ByteArray(s) => {
            let (Some(lower), Some(upper)) = (s.min_opt(), s.max_opt()) else {
                return Ok(ColumnRangeStatistics::Missing);
            };
            convert_byte_array_arrowrs(lower, upper, col_descr)?
        }
        ArrowStatistics::FixedLenByteArray(s) => {
            let (Some(lower), Some(upper)) = (s.min_opt(), s.max_opt()) else {
                return Ok(ColumnRangeStatistics::Missing);
            };
            convert_fixed_len_arrowrs(lower, upper, col_descr)?
        }
    };

    // Cast to ensure that the ColumnRangeStatistics contain the targeted Daft **logical** type
    daft_stats.cast(daft_dtype).context(DaftStatsSnafu)
}

fn convert_int32_arrowrs(
    lower: i32,
    upper: i32,
    col_descr: &ArrowColumnDescriptor,
) -> super::Result<ColumnRangeStatistics> {
    if let Some(ltype) = col_descr.logical_type_ref()
        && matches!(ltype, ArrowLogicalType::Date)
    {
        return make_date_column_range_statistics(lower, upper);
    }

    if col_descr.converted_type() == ArrowConvertedType::DATE {
        return make_date_column_range_statistics(lower, upper);
    }

    // Fallback: plain Int32
    let lower = Int32Array::from_slice("lower", &[lower]).into_series();
    let upper = Int32Array::from_slice("upper", &[upper]).into_series();
    Ok(ColumnRangeStatistics::new(Some(lower), Some(upper))?)
}

fn convert_int64_arrowrs(
    lower: i64,
    upper: i64,
    col_descr: &ArrowColumnDescriptor,
) -> super::Result<ColumnRangeStatistics> {
    if let Some(ArrowLogicalType::Timestamp {
        is_adjusted_to_u_t_c,
        unit,
    }) = col_descr.logical_type_ref()
    {
        let daft_tu = timeunit_to_daft(*unit);
        return make_timestamp_column_range_statistics(
            daft_tu,
            *is_adjusted_to_u_t_c,
            lower,
            upper,
        );
    }

    match col_descr.converted_type() {
        ArrowConvertedType::TIMESTAMP_MICROS => {
            return make_timestamp_column_range_statistics(
                daft_core::datatypes::TimeUnit::Microseconds,
                false,
                lower,
                upper,
            );
        }
        ArrowConvertedType::TIMESTAMP_MILLIS => {
            return make_timestamp_column_range_statistics(
                daft_core::datatypes::TimeUnit::Milliseconds,
                false,
                lower,
                upper,
            );
        }
        _ => {}
    }

    // Fallback: plain Int64
    let lower = Int64Array::from_slice("lower", &[lower]).into_series();
    let upper = Int64Array::from_slice("upper", &[upper]).into_series();
    Ok(ColumnRangeStatistics::new(Some(lower), Some(upper))?)
}

fn convert_int96_arrowrs(
    lower: &ArrowInt96,
    upper: &ArrowInt96,
    col_descr: &ArrowColumnDescriptor,
) -> super::Result<ColumnRangeStatistics> {
    let lower_data = lower.data();
    let upper_data = upper.data();
    let lower_raw: [u32; 3] = [lower_data[0], lower_data[1], lower_data[2]];
    let upper_raw: [u32; 3] = [upper_data[0], upper_data[1], upper_data[2]];

    if let Some(ArrowLogicalType::Timestamp {
        is_adjusted_to_u_t_c,
        unit,
    }) = col_descr.logical_type_ref()
    {
        let daft_tu = timeunit_to_daft(*unit);
        let lower_ts = convert_i96_to_i64_timestamp(lower_raw, daft_tu);
        let upper_ts = convert_i96_to_i64_timestamp(upper_raw, daft_tu);
        return make_timestamp_column_range_statistics(
            daft_tu,
            *is_adjusted_to_u_t_c,
            lower_ts,
            upper_ts,
        );
    }

    match col_descr.converted_type() {
        ArrowConvertedType::TIMESTAMP_MICROS => {
            let tu = daft_core::datatypes::TimeUnit::Microseconds;
            let lower_ts = convert_i96_to_i64_timestamp(lower_raw, tu);
            let upper_ts = convert_i96_to_i64_timestamp(upper_raw, tu);
            return make_timestamp_column_range_statistics(tu, false, lower_ts, upper_ts);
        }
        ArrowConvertedType::TIMESTAMP_MILLIS => {
            let tu = daft_core::datatypes::TimeUnit::Milliseconds;
            let lower_ts = convert_i96_to_i64_timestamp(lower_raw, tu);
            let upper_ts = convert_i96_to_i64_timestamp(upper_raw, tu);
            return make_timestamp_column_range_statistics(tu, false, lower_ts, upper_ts);
        }
        _ => {}
    }

    // INT96 without logical/converted type: return Missing
    Ok(ColumnRangeStatistics::Missing)
}

fn convert_byte_array_arrowrs(
    lower: &ArrowByteArray,
    upper: &ArrowByteArray,
    col_descr: &ArrowColumnDescriptor,
) -> super::Result<ColumnRangeStatistics> {
    let lower_bytes = lower.data();
    let upper_bytes = upper.data();

    if let Some(ltype) = col_descr.logical_type_ref() {
        match ltype {
            ArrowLogicalType::String
            | ArrowLogicalType::Enum
            | ArrowLogicalType::Uuid
            | ArrowLogicalType::Json => {
                let lower_str = String::from_utf8(lower_bytes.to_vec())
                    .context(UnableToParseUtf8FromBinarySnafu)?;
                let upper_str = String::from_utf8(upper_bytes.to_vec())
                    .context(UnableToParseUtf8FromBinarySnafu)?;
                let lower =
                    Utf8Array::from_slice("lower", [lower_str.as_str()].as_slice()).into_series();
                let upper =
                    Utf8Array::from_slice("upper", [upper_str.as_str()].as_slice()).into_series();
                return Ok(ColumnRangeStatistics::new(Some(lower), Some(upper))?);
            }
            ArrowLogicalType::Decimal { precision, scale } => {
                return make_decimal_column_range_statistics(
                    *precision as usize,
                    *scale as usize,
                    lower_bytes,
                    upper_bytes,
                );
            }
            _ => {}
        }
    }

    match col_descr.converted_type() {
        ArrowConvertedType::UTF8 | ArrowConvertedType::ENUM | ArrowConvertedType::JSON => {
            let lower_str = String::from_utf8(lower_bytes.to_vec())
                .context(UnableToParseUtf8FromBinarySnafu)?;
            let upper_str = String::from_utf8(upper_bytes.to_vec())
                .context(UnableToParseUtf8FromBinarySnafu)?;
            let lower =
                Utf8Array::from_slice("lower", [lower_str.as_str()].as_slice()).into_series();
            let upper =
                Utf8Array::from_slice("upper", [upper_str.as_str()].as_slice()).into_series();
            return Ok(ColumnRangeStatistics::new(Some(lower), Some(upper))?);
        }
        ArrowConvertedType::DECIMAL => {
            let p = col_descr.type_precision() as usize;
            let s = col_descr.type_scale() as usize;
            return make_decimal_column_range_statistics(p, s, lower_bytes, upper_bytes);
        }
        _ => {}
    }

    // Fallback: BinaryArray
    let lower = BinaryArray::from_values("lower", std::iter::once(lower_bytes)).into_series();
    let upper = BinaryArray::from_values("upper", std::iter::once(upper_bytes)).into_series();
    Ok(ColumnRangeStatistics::new(Some(lower), Some(upper))?)
}

fn convert_fixed_len_arrowrs(
    lower: &ArrowByteArray,
    upper: &ArrowByteArray,
    col_descr: &ArrowColumnDescriptor,
) -> super::Result<ColumnRangeStatistics> {
    let lower_bytes = lower.data();
    let upper_bytes = upper.data();

    if let Some(ArrowLogicalType::Decimal { precision, scale }) = col_descr.logical_type_ref() {
        return make_decimal_column_range_statistics(
            *precision as usize,
            *scale as usize,
            lower_bytes,
            upper_bytes,
        );
    }

    if col_descr.converted_type() == ArrowConvertedType::DECIMAL {
        let p = col_descr.type_precision() as usize;
        let s = col_descr.type_scale() as usize;
        return make_decimal_column_range_statistics(p, s, lower_bytes, upper_bytes);
    }

    // Fallback: BinaryArray
    let lower = BinaryArray::from_values("lower", std::iter::once(lower_bytes)).into_series();
    let upper = BinaryArray::from_values("upper", std::iter::once(upper_bytes)).into_series();
    Ok(ColumnRangeStatistics::new(Some(lower), Some(upper))?)
}
