use arrow2::array::PrimitiveArray;
use daft_core::{
    datatypes::{
        logical::{DateArray, Decimal128Array, TimestampArray},
        BinaryArray, BooleanArray, Int128Array, Int32Array, Int64Array, Utf8Array,
    },
    IntoSeries, Series,
};
use parquet2::statistics::{BinaryStatistics, BooleanStatistics, PrimitiveStatistics, Statistics};
use snafu::{OptionExt, ResultExt};

use super::ColumnRangeStatistics;
use crate::column_stats::MissingParquetColumnStatisticsSnafu;

use crate::column_stats::UnableToParseUtf8FromBinarySnafu;

impl TryFrom<&BooleanStatistics> for ColumnRangeStatistics {
    type Error = super::Error;
    fn try_from(value: &BooleanStatistics) -> Result<Self, Self::Error> {
        let lower = value
            .min_value
            .context(MissingParquetColumnStatisticsSnafu)?;
        let upper = value
            .max_value
            .context(MissingParquetColumnStatisticsSnafu)?;

        ColumnRangeStatistics::new(
            BooleanArray::from(("lower", [lower].as_slice())).into_series(),
            BooleanArray::from(("upper", [upper].as_slice())).into_series(),
        )
    }
}

impl TryFrom<&BinaryStatistics> for ColumnRangeStatistics {
    type Error = super::Error;

    fn try_from(value: &BinaryStatistics) -> Result<Self, Self::Error> {
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

                    return ColumnRangeStatistics::new(lower, upper);
                }
                PrimitiveLogicalType::Decimal(p, s) => {
                    assert!(lower.len() <= 16);
                    assert!(upper.len() <= 16);
                    let l = crate::utils::deserialize::convert_i128(lower.as_slice(), lower.len());
                    let u = crate::utils::deserialize::convert_i128(upper.as_slice(), upper.len());
                    let lower = Int128Array::from(("lower", [l].as_slice()));
                    let upper = Int128Array::from(("upper", [u].as_slice()));
                    let daft_type = daft_core::datatypes::DataType::Decimal128(p, s);

                    let lower = Decimal128Array::new(
                        daft_core::datatypes::Field::new("lower", daft_type.clone()),
                        lower,
                    )
                    .into_series();
                    let upper = Decimal128Array::new(
                        daft_core::datatypes::Field::new("upper", daft_type),
                        upper,
                    )
                    .into_series();

                    return ColumnRangeStatistics::new(lower, upper);
                }
                _ => todo!("HANDLE BAD LOGICAL TYPE"),
            }
        } else if let Some(ctype) = ptype.converted_type {
            use parquet2::schema::types::PrimitiveConvertedType;
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

                    return ColumnRangeStatistics::new(lower, upper);
                }
                PrimitiveConvertedType::Decimal(p, s) => {
                    assert!(lower.len() <= 16);
                    assert!(upper.len() <= 16);
                    let l = crate::utils::deserialize::convert_i128(lower.as_slice(), lower.len());
                    let u = crate::utils::deserialize::convert_i128(upper.as_slice(), upper.len());
                    let lower = Int128Array::from(("lower", [l].as_slice()));
                    let upper = Int128Array::from(("upper", [u].as_slice()));
                    let daft_type = daft_core::datatypes::DataType::Decimal128(p, s);

                    let lower = Decimal128Array::new(
                        daft_core::datatypes::Field::new("lower", daft_type.clone()),
                        lower,
                    )
                    .into_series();
                    let upper = Decimal128Array::new(
                        daft_core::datatypes::Field::new("upper", daft_type),
                        upper,
                    )
                    .into_series();
                    return ColumnRangeStatistics::new(lower, upper);
                }
                _ => todo!("HANDLE BAD CONVERTED TYPE"),
            }
        }

        let lower = BinaryArray::from(("lower", lower.as_slice())).into_series();
        let upper = BinaryArray::from(("upper", upper.as_slice())).into_series();

        return ColumnRangeStatistics::new(lower, upper);
    }
}

impl<T: parquet2::types::NativeType + daft_core::datatypes::NumericNative>
    TryFrom<&PrimitiveStatistics<T>> for ColumnRangeStatistics
{
    type Error = super::Error;

    fn try_from(value: &PrimitiveStatistics<T>) -> Result<Self, Self::Error> {
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
            use parquet2::schema::types::PhysicalType;
            use parquet2::schema::types::PrimitiveLogicalType;

            match (ptype, ltype) {
                (PhysicalType::Int32, PrimitiveLogicalType::Date) => {
                    let lower = Int32Array::from(("lower", [lower.to_i32().unwrap()].as_slice()));
                    let upper = Int32Array::from(("upper", [upper.to_i32().unwrap()].as_slice()));

                    let dtype = daft_core::datatypes::DataType::Date;

                    let lower = DateArray::new(
                        daft_core::datatypes::Field::new("lower", dtype.clone()),
                        lower,
                    )
                    .into_series();
                    let upper =
                        DateArray::new(daft_core::datatypes::Field::new("upper", dtype), upper)
                            .into_series();
                    return ColumnRangeStatistics::new(lower, upper);
                }
                (
                    PhysicalType::Int64,
                    PrimitiveLogicalType::Timestamp {
                        unit,
                        is_adjusted_to_utc,
                    },
                ) => {
                    let lower = Int64Array::from(("lower", [lower.to_i64().unwrap()].as_slice()));
                    let upper = Int64Array::from(("upper", [upper.to_i64().unwrap()].as_slice()));
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
                    let upper = TimestampArray::new(
                        daft_core::datatypes::Field::new("upper", dtype),
                        upper,
                    )
                    .into_series();
                    return ColumnRangeStatistics::new(lower, upper);
                }

                _ => {}
            }
        }

        // TODO: FIX THESE STATS
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

        return ColumnRangeStatistics::new(lower, upper);
    }
}

impl TryFrom<&dyn Statistics> for ColumnRangeStatistics {
    type Error = super::Error;

    fn try_from(value: &dyn Statistics) -> Result<Self, Self::Error> {
        let ptype = value.physical_type();
        let stats = value.as_any();
        use parquet2::schema::types::PhysicalType;
        match ptype {
            PhysicalType::Boolean => stats
                .downcast_ref::<BooleanStatistics>()
                .unwrap()
                .try_into(),
            PhysicalType::Int32 => stats
                .downcast_ref::<PrimitiveStatistics<i32>>()
                .unwrap()
                .try_into(),
            PhysicalType::Int64 => stats
                .downcast_ref::<PrimitiveStatistics<i64>>()
                .unwrap()
                .try_into(),
            PhysicalType::Int96 => todo!(),
            PhysicalType::Float => stats
                .downcast_ref::<PrimitiveStatistics<f32>>()
                .unwrap()
                .try_into(),
            PhysicalType::Double => stats
                .downcast_ref::<PrimitiveStatistics<f64>>()
                .unwrap()
                .try_into(),
            PhysicalType::ByteArray => stats.downcast_ref::<BinaryStatistics>().unwrap().try_into(),
            PhysicalType::FixedLenByteArray(size) => {
                todo!()
            }
        }
    }
}
