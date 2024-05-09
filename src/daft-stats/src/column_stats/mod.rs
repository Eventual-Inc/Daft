mod arithmetic;
mod comparison;
mod logical;

use std::string::FromUtf8Error;

use daft_core::{
    array::ops::full::FullNull,
    datatypes::{BooleanArray, NullArray},
    DataType, IntoSeries, Series,
};
use snafu::{ResultExt, Snafu};

use crate::DaftCoreComputeSnafu;
#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ColumnRangeStatistics {
    Missing,
    Loaded(Series, Series),
}

#[derive(PartialEq, Debug)]
pub enum TruthValue {
    False,
    Maybe,
    True,
}

impl std::fmt::Display for TruthValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::False => "False",
            Self::Maybe => "Maybe",
            Self::True => "True",
        };

        write!(f, "TruthValue: {value}",)
    }
}

impl ColumnRangeStatistics {
    pub fn new(lower: Option<Series>, upper: Option<Series>) -> crate::Result<Self> {
        match (lower, upper) {
            (Some(l), Some(u)) => {
                assert_eq!(l.len(), 1);
                assert_eq!(u.len(), 1);
                assert_eq!(l.data_type(), u.data_type(), "");

                // If creating on incompatible types, default to `Missing`
                if !ColumnRangeStatistics::supports_dtype(l.data_type()) {
                    return Ok(ColumnRangeStatistics::Missing);
                }

                Ok(ColumnRangeStatistics::Loaded(l, u))
            }
            _ => Ok(ColumnRangeStatistics::Missing),
        }
    }

    pub fn supports_dtype(dtype: &DataType) -> bool {
        match dtype {
            // SUPPORTED TYPES:
            // Null
            DataType::Null |

            // Numeric types
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Int128 |
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
            DataType::Float32 | DataType::Float64 | DataType::Decimal128(..) | DataType::Boolean |

            // String types
            DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(..) |

            // Temporal types
            DataType::Date | DataType::Time(..) | DataType::Timestamp(..) | DataType::Duration(..) => true,

            // UNSUPPORTED TYPES:
            // Types that don't support comparisons and can't be used as ColumnRangeStatistics
            DataType::List(..) | DataType::FixedSizeList(..) | DataType::Image(..) | DataType::FixedShapeImage(..) | DataType::Tensor(..) | DataType::FixedShapeTensor(..) | DataType::Struct(..) | DataType::Map(..) | DataType::Extension(..) | DataType::Embedding(..) | DataType::Unknown => false,
            #[cfg(feature = "python")]
            DataType::Python => false,
        }
    }

    pub fn to_truth_value(&self) -> TruthValue {
        match self {
            Self::Missing => TruthValue::Maybe,
            Self::Loaded(lower, upper) => {
                let lower = lower.bool().unwrap().get(0).unwrap();
                let upper = upper.bool().unwrap().get(0).unwrap();
                match (lower, upper) {
                    (false, false) => TruthValue::False,
                    (false, true) => TruthValue::Maybe,
                    (true, true) => TruthValue::True,
                    (true, false) => panic!("Upper is false and lower is true; Invalid states!"),
                }
            }
        }
    }

    pub fn from_truth_value(tv: TruthValue) -> Self {
        let (lower, upper) = match tv {
            TruthValue::False => (false, false),
            TruthValue::Maybe => (false, true),
            TruthValue::True => (true, true),
        };

        let lower = BooleanArray::from(("lower", [lower].as_slice())).into_series();
        let upper = BooleanArray::from(("upper", [upper].as_slice())).into_series();
        Self::Loaded(lower, upper)
    }

    pub(crate) fn combined_series(&self) -> crate::Result<Series> {
        match self {
            Self::Missing => {
                Ok(NullArray::full_null("null", &daft_core::DataType::Null, 2).into_series())
            }
            Self::Loaded(l, u) => Series::concat([l, u].as_slice()).context(DaftCoreComputeSnafu),
        }
    }

    pub(crate) fn element_size(&self) -> crate::Result<Option<f64>> {
        match self {
            Self::Missing => Ok(None),
            Self::Loaded(l, u) => Ok(Some(
                ((l.size_bytes().context(DaftCoreComputeSnafu)?
                    + u.size_bytes().context(DaftCoreComputeSnafu)?) as f64)
                    / 2.,
            )),
        }
    }

    pub fn from_series(series: &Series) -> Self {
        let lower = series.min(None).unwrap();
        let upper = series.max(None).unwrap();
        let _count = series
            .count(None, daft_core::CountMode::All)
            .unwrap()
            .u64()
            .unwrap()
            .get(0)
            .unwrap() as usize;
        let _null_count = series
            .count(None, daft_core::CountMode::Null)
            .unwrap()
            .u64()
            .unwrap()
            .get(0)
            .unwrap() as usize;
        let _num_bytes = series.size_bytes().unwrap();
        Self::Loaded(lower, upper)
    }

    /// Casts the internal [`Series`] objects to the specified DataType
    pub fn cast(&self, dtype: &DataType) -> crate::Result<Self> {
        match self {
            // `Missing` is casted to `Missing`
            ColumnRangeStatistics::Missing => Ok(ColumnRangeStatistics::Missing),

            // If the type to cast to matches the current type exactly, short-circuit the logic here. This should be the
            // most common case (e.g. parsing a Parquet file with the same types as the inferred types)
            ColumnRangeStatistics::Loaded(l, r) if l.data_type() == dtype => {
                Ok(ColumnRangeStatistics::Loaded(l.clone(), r.clone()))
            }

            // Only certain types are allowed to be casted in the context of ColumnRangeStatistics
            // as casting may not correctly preserve ordering of elements. We allow-list some type combinations
            // but for most combinations, we will default to `ColumnRangeStatistics::Missing`.
            ColumnRangeStatistics::Loaded(l, r) => {
                match (l.data_type(), dtype) {
                    // Int casting to higher bitwidths
                    (DataType::Int8, DataType::Int16) |
                    (DataType::Int8, DataType::Int32) |
                    (DataType::Int8, DataType::Int64) |
                    (DataType::Int16, DataType::Int32) |
                    (DataType::Int16, DataType::Int64) |
                    (DataType::Int32, DataType::Int64) |
                    // UInt casting to higher bitwidths
                    (DataType::UInt8, DataType::UInt16) |
                    (DataType::UInt8, DataType::UInt32) |
                    (DataType::UInt8, DataType::UInt64) |
                    (DataType::UInt16, DataType::UInt32) |
                    (DataType::UInt16, DataType::UInt64) |
                    (DataType::UInt32, DataType::UInt64) |
                    // Float casting to higher bitwidths
                    (DataType::Float32, DataType::Float64) |
                    // Numeric to temporal casting from smaller-than-eq bitwidths
                    (DataType::Int8, DataType::Date) |
                    (DataType::Int16, DataType::Date) |
                    (DataType::Int32, DataType::Date) |
                    (DataType::Int8, DataType::Timestamp(..)) |
                    (DataType::Int16, DataType::Timestamp(..)) |
                    (DataType::Int32, DataType::Timestamp(..)) |
                    (DataType::Int64, DataType::Timestamp(..)) |
                    // Binary to Utf8
                    (DataType::Binary, DataType::Utf8)
                    => Ok(ColumnRangeStatistics::Loaded(
                        l.cast(dtype).context(DaftCoreComputeSnafu)?,
                        r.cast(dtype).context(DaftCoreComputeSnafu)?,
                    )),
                    _ => Ok(ColumnRangeStatistics::Missing)
                }
            }
        }
    }
}

impl std::fmt::Display for ColumnRangeStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing => write!(f, "ColumnRangeStatistics: Missing"),
            Self::Loaded(lower, upper) => write!(
                f,
                "ColumnRangeStatistics:
lower:\n{}
upper:\n{}
    ",
                lower, upper
            ),
        }
    }
}

impl std::fmt::Debug for ColumnRangeStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl TryFrom<&daft_dsl::LiteralValue> for ColumnRangeStatistics {
    type Error = crate::Error;
    fn try_from(value: &daft_dsl::LiteralValue) -> crate::Result<Self, Self::Error> {
        let series = value.to_series();
        assert_eq!(series.len(), 1);
        Self::new(Some(series.clone()), Some(series.clone()))
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("MissingParquetColumnStatistics"))]
    MissingParquetColumnStatistics {},
    #[snafu(display("UnableToParseUtf8FromBinary: {source}"))]
    UnableToParseUtf8FromBinary { source: FromUtf8Error },
}

impl From<Error> for crate::Error {
    fn from(value: Error) -> Self {
        crate::Error::MissingStatistics { source: value }
    }
}

#[cfg(test)]
mod test {

    use daft_core::{array::ops::DaftCompare, datatypes::Int32Array, IntoSeries};

    use crate::column_stats::TruthValue;

    use super::ColumnRangeStatistics;

    #[test]
    fn test_equal() -> crate::Result<()> {
        let l = ColumnRangeStatistics::new(
            Some(Int32Array::from(("l", vec![1])).into_series()),
            Some(Int32Array::from(("l", vec![5])).into_series()),
        )?;
        let r = ColumnRangeStatistics::new(
            Some(Int32Array::from(("r", vec![4])).into_series()),
            Some(Int32Array::from(("r", vec![4])).into_series()),
        )?;
        assert_eq!(l.lt(&r)?.to_truth_value(), TruthValue::Maybe);

        Ok(())
    }
}
