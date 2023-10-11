use arrow2::array::PrimitiveArray;
use daft_core::{
    datatypes::{
        logical::{DateArray, Decimal128Array, TimestampArray},
        BinaryArray, BooleanArray, Int128Array, Int32Array, Int64Array, Utf8Array,
    },
    IntoSeries, Series,
};
use parquet2::statistics::{BinaryStatistics, BooleanStatistics, PrimitiveStatistics, Statistics};

use super::ColumnRangeStatistics;

impl From<(&BooleanStatistics)> for ColumnRangeStatistics {
    fn from(value: &BooleanStatistics) -> Self {
        let lower = value.min_value.unwrap();
        let upper = value.max_value.unwrap();

        ColumnRangeStatistics {
            lower: BooleanArray::from(("lower", [lower].as_slice())).into_series(),
            upper: BooleanArray::from(("upper", [upper].as_slice())).into_series(),
        }
    }
}

impl From<(&BinaryStatistics)> for ColumnRangeStatistics {
    fn from(value: &BinaryStatistics) -> Self {
        let lower = value.min_value.as_ref().unwrap();
        let upper = value.max_value.as_ref().unwrap();
        let ptype = &value.primitive_type;

        if let Some(ltype) = ptype.logical_type {
            use parquet2::schema::types::PrimitiveLogicalType;
            match ltype {
                PrimitiveLogicalType::String
                | PrimitiveLogicalType::Enum
                | PrimitiveLogicalType::Uuid
                | PrimitiveLogicalType::Json => {
                    let lower = String::from_utf8(lower.clone()).unwrap();
                    let upper = String::from_utf8(upper.clone()).unwrap();

                    let lower =
                        Utf8Array::from(("lower", [lower.as_str()].as_slice())).into_series();
                    let upper =
                        Utf8Array::from(("upper", [upper.as_str()].as_slice())).into_series();

                    return ColumnRangeStatistics { lower, upper };
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

                    return ColumnRangeStatistics { lower, upper };
                }
                _ => todo!("HANDLE BAD LOGICAL TYPE"),
            }
        } else if let Some(ctype) = ptype.converted_type {
            use parquet2::schema::types::PrimitiveConvertedType;
            match ctype {
                PrimitiveConvertedType::Utf8
                | PrimitiveConvertedType::Enum
                | PrimitiveConvertedType::Json => {
                    let lower = String::from_utf8(lower.clone()).unwrap();
                    let upper = String::from_utf8(upper.clone()).unwrap();

                    let lower =
                        Utf8Array::from(("lower", [lower.as_str()].as_slice())).into_series();
                    let upper =
                        Utf8Array::from(("upper", [upper.as_str()].as_slice())).into_series();

                    return ColumnRangeStatistics { lower, upper };
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
                    return ColumnRangeStatistics { lower, upper };
                }
                _ => todo!("HANDLE BAD CONVERTED TYPE"),
            }
        }

        let lower = BinaryArray::from(("lower", lower.as_slice())).into_series();
        let upper = BinaryArray::from(("upper", upper.as_slice())).into_series();

        return ColumnRangeStatistics { lower, upper };
    }
}

impl<T: parquet2::types::NativeType + daft_core::datatypes::NumericNative>
    From<(&PrimitiveStatistics<T>)> for ColumnRangeStatistics
{
    fn from(value: &PrimitiveStatistics<T>) -> Self {
        // TODO: dont unwrap
        let lower = value.min_value.unwrap();
        let upper = value.max_value.unwrap();

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
                    return ColumnRangeStatistics { lower, upper };
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
                    return ColumnRangeStatistics { lower, upper };
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

        ColumnRangeStatistics { lower, upper }
    }
}

impl From<&dyn Statistics> for ColumnRangeStatistics {
    fn from(value: &dyn Statistics) -> Self {
        let ptype = value.physical_type();
        let stats = value.as_any();
        use parquet2::schema::types::PhysicalType;
        match ptype {
            PhysicalType::Boolean => stats.downcast_ref::<BooleanStatistics>().unwrap().into(),
            PhysicalType::Int32 => stats
                .downcast_ref::<PrimitiveStatistics<i32>>()
                .unwrap()
                .into(),
            PhysicalType::Int64 => stats
                .downcast_ref::<PrimitiveStatistics<i64>>()
                .unwrap()
                .into(),
            PhysicalType::Int96 => todo!(),
            PhysicalType::Float => stats
                .downcast_ref::<PrimitiveStatistics<f32>>()
                .unwrap()
                .into(),
            PhysicalType::Double => stats
                .downcast_ref::<PrimitiveStatistics<f64>>()
                .unwrap()
                .into(),
            PhysicalType::ByteArray => stats.downcast_ref::<BinaryStatistics>().unwrap().into(),
            PhysicalType::FixedLenByteArray(size) => {
                todo!()
            }
        }
    }
}
