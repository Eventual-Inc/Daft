mod arithmetic;
mod comparison;
mod logical;

use std::string::FromUtf8Error;

use daft_core::{
    array::ops::full::FullNull,
    datatypes::{BooleanArray, NullArray},
    IntoSeries, Series,
};
use snafu::{ResultExt, Snafu};

use crate::DaftCoreComputeSnafu;
#[derive(Clone, serde::Serialize, serde::Deserialize)]
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
                assert_eq!(l.data_type(), u.data_type());
                Ok(ColumnRangeStatistics::Loaded(l, u))
            }
            _ => Ok(ColumnRangeStatistics::Missing),
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

    pub(crate) fn element_size(&self) -> crate::Result<usize> {
        match self {
            Self::Missing => Ok(0),
            Self::Loaded(l, u) => Ok((l.size_bytes().context(DaftCoreComputeSnafu)?
                + u.size_bytes().context(DaftCoreComputeSnafu)?)
                / 2),
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
        let ser = value.to_series();
        assert_eq!(ser.len(), 1);
        Self::new(Some(ser.clone()), Some(ser.clone()))
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
