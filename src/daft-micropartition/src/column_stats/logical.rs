use snafu::ResultExt;

use super::{ColumnRangeStatistics, TruthValue};

use crate::DaftCoreComputeSnafu;

impl std::ops::Not for &ColumnRangeStatistics {
    type Output = crate::Result<ColumnRangeStatistics>;
    fn not(self) -> Self::Output {
        match self {
            ColumnRangeStatistics::Missing => Ok(ColumnRangeStatistics::Missing),
            ColumnRangeStatistics::Loaded(lower, upper) => {
                let new_lower = upper.not().context(DaftCoreComputeSnafu)?;
                let new_upper = lower.not().context(DaftCoreComputeSnafu)?;

                Ok(ColumnRangeStatistics::Loaded(new_lower, new_upper))
            }
        }
    }
}

impl std::ops::BitAnd for &ColumnRangeStatistics {
    type Output = crate::Result<ColumnRangeStatistics>;
    fn bitand(self, rhs: Self) -> Self::Output {
        // +-------+-------+-------+-------+
        // | Value | False | Maybe | True  |
        // +-------+-------+-------+-------+
        // | False | False | False | False |
        // | Maybe | False | Maybe | Maybe |
        // | True  | False | Maybe | True  |
        // +-------+-------+-------+-------+

        let lt = self.to_truth_value();
        let rt = rhs.to_truth_value();
        use TruthValue::*;
        let nv = match (lt, rt) {
            (False, _) => False,
            (_, False) => False,
            (Maybe, _) => Maybe,
            (_, Maybe) => Maybe,
            (True, True) => True,
        };
        Ok(ColumnRangeStatistics::from_truth_value(nv))
    }
}

impl std::ops::BitOr for &ColumnRangeStatistics {
    type Output = crate::Result<ColumnRangeStatistics>;
    fn bitor(self, rhs: Self) -> Self::Output {
        // +-------+-------+-------+------+
        // | Value | False | Maybe | True |
        // +-------+-------+-------+------+
        // | False | False | Maybe | True |
        // | Maybe | Maybe | Maybe | True |
        // | True  | True  | True  | True |
        // +-------+-------+-------+------+
        let lt = self.to_truth_value();
        let rt = rhs.to_truth_value();
        use TruthValue::*;
        let nv = match (lt, rt) {
            (False, False) => False,
            (True, _) => True,
            (_, True) => True,
            (Maybe, _) => Maybe,
            (_, Maybe) => Maybe,
        };
        Ok(ColumnRangeStatistics::from_truth_value(nv))
    }
}
