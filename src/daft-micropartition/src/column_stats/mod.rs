mod arithmetic;
mod comparison;
mod logical;

use daft_core::{datatypes::BooleanArray, IntoSeries, Series};
#[derive(Clone)]
pub(crate) struct ColumnRangeStatistics {
    pub lower: Series,
    pub upper: Series,
}

struct ColumnMetadata {
    pub range_statistics: ColumnRangeStatistics,
    pub count: usize,
    pub null_count: usize,
    pub num_bytes: usize,
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
    pub fn to_truth_value(&self) -> TruthValue {
        let lower = self.lower.bool().unwrap().get(0).unwrap();
        let upper = self.upper.bool().unwrap().get(0).unwrap();
        match (lower, upper) {
            (false, false) => TruthValue::False,
            (false, true) => TruthValue::Maybe,
            (true, true) => TruthValue::True,
            (true, false) => panic!("Upper is false and lower is true; Invalid states!"),
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
        Self { lower, upper }
    }

    pub fn from_series(series: &Series) -> Self {
        let lower = series.min(None).unwrap();
        let upper = series.max(None).unwrap();
        let count = series
            .count(None, daft_core::CountMode::All)
            .unwrap()
            .u64()
            .unwrap()
            .get(0)
            .unwrap() as usize;
        let null_count = series
            .count(None, daft_core::CountMode::Null)
            .unwrap()
            .u64()
            .unwrap()
            .get(0)
            .unwrap() as usize;
        let num_bytes = series.size_bytes().unwrap();
        Self { lower, upper }
    }
}

impl std::fmt::Display for ColumnRangeStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ColumnRangeStatistics:
lower:\n{}
upper:\n{}
",
            self.lower, self.upper
        )
    }
}

impl std::fmt::Debug for ColumnRangeStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl TryFrom<&daft_dsl::LiteralValue> for ColumnRangeStatistics {
    type Error = crate::Error;
    fn try_from(value: &daft_dsl::LiteralValue) -> Result<Self, Self::Error> {
        let ser = value.to_series();
        assert_eq!(ser.len(), 1);
        Ok(Self {
            lower: ser.clone(),
            upper: ser.clone(),
        })
    }
}

#[cfg(test)]
mod test {
    use common_error::DaftResult;
    use daft_core::{
        array::ops::DaftCompare,
        datatypes::{Int32Array, Int64Array},
        IntoSeries,
    };

    use super::ColumnRangeStatistics;

    #[test]
    fn test_equal() -> crate::Result<()> {
        let l = ColumnRangeStatistics {
            lower: Int64Array::from(("l", vec![1])).into_series(),
            upper: Int64Array::from(("l", vec![5])).into_series(),
        };
        let r = ColumnRangeStatistics {
            lower: Int32Array::from(("r", vec![4])).into_series(),
            upper: Int32Array::from(("r", vec![6])).into_series(),
        };
        println!("{l}");
        println!("{r}");
        println!("{}", l.lt(&r)?.to_truth_value());

        Ok(())
    }
}
