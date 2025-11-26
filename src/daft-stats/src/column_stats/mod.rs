mod arithmetic;
mod comparison;
mod logical;

use std::{
    hash::{Hash, Hasher},
    string::FromUtf8Error,
};

use daft_core::prelude::*;
use snafu::{ResultExt, Snafu};

use crate::DaftCoreComputeSnafu;
#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ColumnRangeStatistics {
    Missing,
    Loaded(Series, Series),
}

impl Hash for ColumnRangeStatistics {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Missing => (),
            Self::Loaded(l, u) => {
                let lower_hashes = l
                    .hash(None)
                    .expect("Failed to hash lower column range statistics");
                lower_hashes.into_iter().for_each(|h| h.hash(state));
                let upper_hashes = u
                    .hash(None)
                    .expect("Failed to hash upper column range statistics");
                upper_hashes.into_iter().for_each(|h| h.hash(state));
            }
        }
    }
}

#[derive(PartialEq, Eq)]
pub enum TruthValue {
    False,
    Maybe,
    True,
}
impl std::fmt::Debug for TruthValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
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
                if !Self::supports_dtype(l.data_type()) {
                    return Ok(Self::Missing);
                }

                Ok(Self::Loaded(l, u))
            }
            _ => Ok(Self::Missing),
        }
    }

    #[must_use]
    pub fn supports_dtype(dtype: &DataType) -> bool {
        match dtype {
            // SUPPORTED TYPES:
            // Null
            DataType::Null |

            // Numeric types
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
            DataType::Float32 | DataType::Float64 | DataType::Decimal128(..) | DataType::Boolean |

            // String types
            DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(..) |

            // Temporal types
            DataType::Date | DataType::Time(..) | DataType::Timestamp(..) | DataType::Duration(..) | DataType::Interval => true,

            // UNSUPPORTED TYPES:
            // Types that don't support comparisons and can't be used as ColumnRangeStatistics
            DataType::List(..) | DataType::FixedSizeList(..) | DataType::Image(..) | DataType::FixedShapeImage(..) | DataType::Tensor(..) | DataType::SparseTensor(..) | DataType::FixedShapeSparseTensor(..) | DataType::FixedShapeTensor(..) | DataType::Struct(..) | DataType::Map { .. } | DataType::Extension(..) | DataType::Embedding(..) | DataType::Unknown | DataType::File(_) => false,
            #[cfg(feature = "python")]
            DataType::Python => false,
        }
    }

    #[must_use]
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

    #[must_use]
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
            Self::Missing => Ok(NullArray::full_null("null", &DataType::Null, 2).into_series()),
            Self::Loaded(l, u) => Series::concat([l, u].as_slice()).context(DaftCoreComputeSnafu),
        }
    }

    pub(crate) fn element_size(&self) -> crate::Result<Option<f64>> {
        match self {
            Self::Missing => Ok(None),
            Self::Loaded(l, u) => Ok(Some(((l.size_bytes() + u.size_bytes()) as f64) / 2.)),
        }
    }

    #[must_use]
    pub fn from_series(series: &Series) -> Self {
        let lower = series.min(None).unwrap();
        let upper = series.max(None).unwrap();
        let _count = series
            .count(None, CountMode::All)
            .unwrap()
            .u64()
            .unwrap()
            .get(0)
            .unwrap() as usize;
        let _null_count = series
            .count(None, CountMode::Null)
            .unwrap()
            .u64()
            .unwrap()
            .get(0)
            .unwrap() as usize;
        Self::Loaded(lower, upper)
    }

    /// Casts the internal [`Series`] objects to the specified DataType
    pub fn cast(&self, dtype: &DataType) -> crate::Result<Self> {
        match self {
            // `Missing` is casted to `Missing`
            Self::Missing => Ok(Self::Missing),

            // If the type to cast to matches the current type exactly, short-circuit the logic here. This should be the
            // most common case (e.g. parsing a Parquet file with the same types as the inferred types)
            Self::Loaded(l, r) if l.data_type() == dtype => Ok(Self::Loaded(l.clone(), r.clone())),

            // Only certain types are allowed to be casted in the context of ColumnRangeStatistics
            // as casting may not correctly preserve ordering of elements. We allow-list some type combinations
            // but for most combinations, we will default to `ColumnRangeStatistics::Missing`.
            Self::Loaded(l, r) => {
                match (l.data_type(), dtype) {
                    // Int casting to higher bitwidths
                    (
                        DataType::Int8,
                        DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::Date
                        | DataType::Timestamp(..),
                    )
                    | (
                        DataType::Int16,
                        DataType::Int32
                        | DataType::Int64
                        | DataType::Date
                        | DataType::Timestamp(..),
                    )
                    | (
                        DataType::Int32,
                        DataType::Int64 | DataType::Date | DataType::Timestamp(..),
                    )
                    | (DataType::UInt8, DataType::UInt16 | DataType::UInt32 | DataType::UInt64)
                    | (DataType::UInt16, DataType::UInt32 | DataType::UInt64)
                    | (DataType::UInt32, DataType::UInt64)
                    | (DataType::Float32, DataType::Float64)
                    | (DataType::Int64, DataType::Timestamp(..))
                    | (DataType::Binary, DataType::Utf8) => Ok(Self::Loaded(
                        l.cast(dtype).context(DaftCoreComputeSnafu)?,
                        r.cast(dtype).context(DaftCoreComputeSnafu)?,
                    )),
                    _ => Ok(Self::Missing),
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
lower:\n{lower}
upper:\n{upper}
    "
            ),
        }
    }
}

impl std::fmt::Debug for ColumnRangeStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl TryFrom<daft_core::lit::Literal> for ColumnRangeStatistics {
    type Error = crate::Error;
    fn try_from(value: daft_core::lit::Literal) -> crate::Result<Self, Self::Error> {
        let series: Series = value.into();
        assert_eq!(series.len(), 1);
        Self::new(Some(series.clone()), Some(series))
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
        Self::MissingStatistics { source: value }
    }
}

#[cfg(test)]
mod test {

    use daft_core::prelude::*;

    use super::ColumnRangeStatistics;
    use crate::column_stats::TruthValue;

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
