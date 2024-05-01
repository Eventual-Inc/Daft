use arrow2::array::PrimitiveArray;
use daft_core::{
    datatypes::{
        logical::{DateArray, Decimal128Array, TimestampArray},
        BinaryArray, BooleanArray, Int128Array, Int32Array, Int64Array, Utf8Array,
    },
    DataType, IntoSeries, Series,
};
use daft_stats::ColumnRangeStatistics;
use parquet2::{
    schema::types::{PhysicalType, PrimitiveConvertedType, TimeUnit},
    statistics::{
        BinaryStatistics, BooleanStatistics, FixedLenStatistics, PrimitiveStatistics, Statistics,
    },
};
use snafu::{OptionExt, ResultExt};

use super::{DaftStatsSnafu, MissingParquetColumnStatisticsSnafu, Wrap};

use super::utils::*;
use super::UnableToParseUtf8FromBinarySnafu;

impl TryFrom<&BooleanStatistics> for Wrap<ColumnRangeStatistics> {
    type Error = super::Error;
    fn try_from(value: &BooleanStatistics) -> Result<Self, Self::Error> {
        if let Some(lower) = value.min_value
            && let Some(upper) = value.max_value
        {
            Ok(ColumnRangeStatistics::new(
                Some(BooleanArray::from(("lower", [lower].as_slice())).into_series()),
                Some(BooleanArray::from(("upper", [upper].as_slice())).into_series()),
            )?
            .into())
        } else {
            Ok(ColumnRangeStatistics::Missing.into())
        }
    }
}

impl TryFrom<&BinaryStatistics> for Wrap<ColumnRangeStatistics> {
    type Error = super::Error;

    fn try_from(value: &BinaryStatistics) -> Result<Self, Self::Error> {
        if value.min_value.is_none() || value.max_value.is_none() {
            return Ok(ColumnRangeStatistics::Missing.into());
        }

        let lower = value
            .min_value
            .as_ref()
            .context(MissingParquetColumnStatisticsSnafu)?;
        let upper = value
            .max_value
            .as_ref()
            .context(MissingParquetColumnStatisticsSnafu)?;
        let ptype = &value.primitive_type;

        if let Some(ltype) = ptype.logical_type {
            use parquet2::schema::types::PrimitiveLogicalType;
            match ltype {
                PrimitiveLogicalType::String
                | PrimitiveLogicalType::Enum
                | PrimitiveLogicalType::Uuid
                | PrimitiveLogicalType::Json => {
                    let lower = String::from_utf8(lower.clone())
                        .context(UnableToParseUtf8FromBinarySnafu)?;
                    let upper = String::from_utf8(upper.clone())
                        .context(UnableToParseUtf8FromBinarySnafu)?;

                    let lower =
                        Utf8Array::from(("lower", [lower.as_str()].as_slice())).into_series();
                    let upper =
                        Utf8Array::from(("upper", [upper.as_str()].as_slice())).into_series();

                    return Ok(ColumnRangeStatistics::new(Some(lower), Some(upper))?.into());
                }
                PrimitiveLogicalType::Decimal(p, s) => {
                    return Ok(make_decimal_column_range_statistics(
                        p,
                        s,
                        lower.as_slice(),
                        upper.as_slice(),
                    )?
                    .into())
                }
                _ => {} // fall back
            }
        } else if let Some(ctype) = ptype.converted_type {
            match ctype {
                PrimitiveConvertedType::Utf8
                | PrimitiveConvertedType::Enum
                | PrimitiveConvertedType::Json => {
                    let lower = String::from_utf8(lower.clone())
                        .context(UnableToParseUtf8FromBinarySnafu)?;
                    let upper = String::from_utf8(upper.clone())
                        .context(UnableToParseUtf8FromBinarySnafu)?;

                    let lower =
                        Utf8Array::from(("lower", [lower.as_str()].as_slice())).into_series();
                    let upper =
                        Utf8Array::from(("upper", [upper.as_str()].as_slice())).into_series();

                    return Ok(ColumnRangeStatistics::new(Some(lower), Some(upper))?.into());
                }
                PrimitiveConvertedType::Decimal(p, s) => {
                    return Ok(make_decimal_column_range_statistics(
                        p,
                        s,
                        lower.as_slice(),
                        upper.as_slice(),
                    )?
                    .into())
                }
                _ => {} // fall back
            }
        }

        let lower = BinaryArray::from(("lower", lower.as_slice())).into_series();
        let upper = BinaryArray::from(("upper", upper.as_slice())).into_series();

        Ok(ColumnRangeStatistics::new(Some(lower), Some(upper))?.into())
    }
}

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
    let lower = Int128Array::from(("lower", [l].as_slice()));
    let upper = Int128Array::from(("upper", [u].as_slice()));
    let daft_type = daft_core::datatypes::DataType::Decimal128(p, s);

    let lower = Decimal128Array::new(
        daft_core::datatypes::Field::new("lower", daft_type.clone()),
        lower,
    )
    .into_series();
    let upper = Decimal128Array::new(daft_core::datatypes::Field::new("upper", daft_type), upper)
        .into_series();

    Ok(ColumnRangeStatistics::new(Some(lower), Some(upper))?)
}

fn make_date_column_range_statistics(
    lower: i32,
    upper: i32,
) -> super::Result<ColumnRangeStatistics> {
    let lower = Int32Array::from(("lower", [lower].as_slice()));
    let upper = Int32Array::from(("upper", [upper].as_slice()));

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
    unit: parquet2::schema::types::TimeUnit,
    is_adjusted_to_utc: bool,
    lower: i64,
    upper: i64,
) -> super::Result<ColumnRangeStatistics> {
    let lower = Int64Array::from(("lower", [lower].as_slice()));
    let upper = Int64Array::from(("upper", [upper].as_slice()));
    let tu = match unit {
        parquet2::schema::types::TimeUnit::Nanoseconds => {
            daft_core::datatypes::TimeUnit::Nanoseconds
        }
        parquet2::schema::types::TimeUnit::Microseconds => {
            daft_core::datatypes::TimeUnit::Microseconds
        }
        parquet2::schema::types::TimeUnit::Milliseconds => {
            daft_core::datatypes::TimeUnit::Milliseconds
        }
    };
    let tz = if is_adjusted_to_utc {
        Some("+00:00".to_string())
    } else {
        None
    };

    let dtype = daft_core::datatypes::DataType::Timestamp(tu, tz);

    let lower = TimestampArray::new(
        daft_core::datatypes::Field::new("lower", dtype.clone()),
        lower,
    )
    .into_series();
    let upper =
        TimestampArray::new(daft_core::datatypes::Field::new("upper", dtype), upper).into_series();
    Ok(ColumnRangeStatistics::new(Some(lower), Some(upper))?)
}

impl TryFrom<&FixedLenStatistics> for Wrap<ColumnRangeStatistics> {
    type Error = super::Error;

    fn try_from(value: &FixedLenStatistics) -> Result<Self, Self::Error> {
        if value.min_value.is_none() || value.max_value.is_none() {
            return Ok(ColumnRangeStatistics::Missing.into());
        }

        let lower = value
            .min_value
            .as_ref()
            .context(MissingParquetColumnStatisticsSnafu)?;
        let upper = value
            .max_value
            .as_ref()
            .context(MissingParquetColumnStatisticsSnafu)?;
        let ptype = &value.primitive_type;

        if let Some(ltype) = ptype.logical_type {
            use parquet2::schema::types::PrimitiveLogicalType;
            if let PrimitiveLogicalType::Decimal(p, s) = ltype {
                return Ok(make_decimal_column_range_statistics(
                    p,
                    s,
                    lower.as_slice(),
                    upper.as_slice(),
                )?
                .into());
            }
        } else if let Some(PrimitiveConvertedType::Decimal(p, s)) = ptype.converted_type {
            return Ok(make_decimal_column_range_statistics(
                p,
                s,
                lower.as_slice(),
                upper.as_slice(),
            )?
            .into());
        }

        let lower = BinaryArray::from(("lower", lower.as_slice())).into_series();
        let upper = BinaryArray::from(("upper", upper.as_slice())).into_series();

        Ok(ColumnRangeStatistics::new(Some(lower), Some(upper))?.into())
    }
}

impl<T: parquet2::types::NativeType + daft_core::datatypes::NumericNative>
    TryFrom<&PrimitiveStatistics<T>> for Wrap<ColumnRangeStatistics>
{
    type Error = super::Error;

    fn try_from(value: &PrimitiveStatistics<T>) -> Result<Self, Self::Error> {
        if value.min_value.is_none() || value.max_value.is_none() {
            return Ok(ColumnRangeStatistics::Missing.into());
        }

        let lower = value
            .min_value
            .context(MissingParquetColumnStatisticsSnafu)?;
        let upper = value
            .max_value
            .context(MissingParquetColumnStatisticsSnafu)?;

        let prim_type = &value.primitive_type;
        let ptype = prim_type.physical_type;

        if let Some(ltype) = prim_type.logical_type {
            /// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
            use parquet2::schema::types::PrimitiveLogicalType;

            match (ptype, ltype) {
                (PhysicalType::Int32, PrimitiveLogicalType::Date) => {
                    return Ok(make_date_column_range_statistics(
                        lower.to_i32().unwrap(),
                        upper.to_i32().unwrap(),
                    )?
                    .into())
                }
                (
                    PhysicalType::Int64,
                    PrimitiveLogicalType::Timestamp {
                        unit,
                        is_adjusted_to_utc,
                    },
                ) => {
                    return Ok(make_timestamp_column_range_statistics(
                        unit,
                        is_adjusted_to_utc,
                        lower.to_i64().unwrap(),
                        upper.to_i64().unwrap(),
                    )?
                    .into())
                }
                _ => {} // fall back
            }
        } else if let Some(ctype) = prim_type.converted_type {
            match (ptype, ctype) {
                (PhysicalType::Int32, PrimitiveConvertedType::Date) => {
                    return Ok(make_date_column_range_statistics(
                        lower.to_i32().unwrap(),
                        upper.to_i32().unwrap(),
                    )?
                    .into())
                }
                (PhysicalType::Int64, PrimitiveConvertedType::TimestampMicros) => {
                    let unit = TimeUnit::Microseconds;
                    return Ok(make_timestamp_column_range_statistics(
                        unit,
                        false,
                        lower.to_i64().unwrap(),
                        upper.to_i64().unwrap(),
                    )?
                    .into());
                }
                (PhysicalType::Int64, PrimitiveConvertedType::TimestampMillis) => {
                    let unit = TimeUnit::Milliseconds;
                    return Ok(make_timestamp_column_range_statistics(
                        unit,
                        false,
                        lower.to_i64().unwrap(),
                        upper.to_i64().unwrap(),
                    )?
                    .into());
                }
                _ => {}
            }
        }
        // fall back case
        let lower = Series::try_from((
            "lower",
            Box::new(PrimitiveArray::<T>::from_vec(vec![lower])) as Box<dyn arrow2::array::Array>,
        ))
        .unwrap();
        let upper = Series::try_from((
            "upper",
            Box::new(PrimitiveArray::<T>::from_vec(vec![upper])) as Box<dyn arrow2::array::Array>,
        ))
        .unwrap();

        Ok(ColumnRangeStatistics::new(Some(lower), Some(upper))?.into())
    }
}

fn convert_int96_column_range_statistics(
    value: &PrimitiveStatistics<[u32; 3]>,
) -> super::Result<ColumnRangeStatistics> {
    if value.min_value.is_none() || value.max_value.is_none() {
        return Ok(ColumnRangeStatistics::Missing);
    }

    let lower = value
        .min_value
        .context(MissingParquetColumnStatisticsSnafu)?;
    let upper = value
        .max_value
        .context(MissingParquetColumnStatisticsSnafu)?;

    let prim_type = &value.primitive_type;

    if let Some(ltype) = prim_type.logical_type {
        use parquet2::schema::types::PrimitiveLogicalType;
        if let PrimitiveLogicalType::Timestamp {
            unit,
            is_adjusted_to_utc,
        } = ltype
        {
            let lower = convert_i96_to_i64_timestamp(lower, unit);
            let upper = convert_i96_to_i64_timestamp(upper, unit);
            return make_timestamp_column_range_statistics(unit, is_adjusted_to_utc, lower, upper);
        }
    } else if let Some(ctype) = prim_type.converted_type {
        match ctype {
            PrimitiveConvertedType::TimestampMicros => {
                let unit = TimeUnit::Microseconds;
                let lower = convert_i96_to_i64_timestamp(lower, unit);
                let upper = convert_i96_to_i64_timestamp(upper, unit);
                return make_timestamp_column_range_statistics(unit, false, lower, upper);
            }
            PrimitiveConvertedType::TimestampMillis => {
                let unit = TimeUnit::Milliseconds;
                let lower = convert_i96_to_i64_timestamp(lower, unit);
                let upper = convert_i96_to_i64_timestamp(upper, unit);
                return make_timestamp_column_range_statistics(unit, false, lower, upper);
            }
            _ => {}
        }
    }

    Ok(ColumnRangeStatistics::Missing)
}

pub(crate) fn parquet_statistics_to_column_range_statistics(
    pq_stats: &dyn Statistics,
    daft_dtype: &DataType,
) -> Result<ColumnRangeStatistics, super::Error> {
    // Create ColumnRangeStatistics containing Series objects that are the **physical** types parsed from Parquet
    let ptype = pq_stats.physical_type();
    let stats = pq_stats.as_any();
    let daft_stats = match ptype {
        PhysicalType::Boolean => stats
            .downcast_ref::<BooleanStatistics>()
            .unwrap()
            .try_into()
            .map(|wrap: Wrap<ColumnRangeStatistics>| wrap.0),
        PhysicalType::Int32 => stats
            .downcast_ref::<PrimitiveStatistics<i32>>()
            .unwrap()
            .try_into()
            .map(|wrap: Wrap<ColumnRangeStatistics>| wrap.0),
        PhysicalType::Int64 => stats
            .downcast_ref::<PrimitiveStatistics<i64>>()
            .unwrap()
            .try_into()
            .map(|wrap: Wrap<ColumnRangeStatistics>| wrap.0),
        PhysicalType::Int96 => Ok(convert_int96_column_range_statistics(
            stats
                .downcast_ref::<PrimitiveStatistics<[u32; 3]>>()
                .unwrap(),
        )?),
        PhysicalType::Float => stats
            .downcast_ref::<PrimitiveStatistics<f32>>()
            .unwrap()
            .try_into()
            .map(|wrap: Wrap<ColumnRangeStatistics>| wrap.0),
        PhysicalType::Double => stats
            .downcast_ref::<PrimitiveStatistics<f64>>()
            .unwrap()
            .try_into()
            .map(|wrap: Wrap<ColumnRangeStatistics>| wrap.0),
        PhysicalType::ByteArray => stats
            .downcast_ref::<BinaryStatistics>()
            .unwrap()
            .try_into()
            .map(|wrap: Wrap<ColumnRangeStatistics>| wrap.0),
        PhysicalType::FixedLenByteArray(_) => stats
            .downcast_ref::<FixedLenStatistics>()
            .unwrap()
            .try_into()
            .map(|wrap: Wrap<ColumnRangeStatistics>| wrap.0),
    };

    // Cast to ensure that the ColumnRangeStatistics now contain the targeted Daft **logical** type
    daft_stats.and_then(|s| s.cast(daft_dtype).context(DaftStatsSnafu))
}
