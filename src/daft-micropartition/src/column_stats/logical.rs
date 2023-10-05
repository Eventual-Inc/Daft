use snafu::ResultExt;

use super::{ColumnStatistics, TruthValue};

use crate::DaftCoreComputeSnafu;

impl std::ops::Not for &ColumnStatistics {
    type Output = crate::Result<ColumnStatistics>;
    fn not(self) -> Self::Output {
        let lower = (&self.upper).not().context(DaftCoreComputeSnafu)?;
        let upper = (&self.lower).not().context(DaftCoreComputeSnafu)?;

        Ok(ColumnStatistics {
            lower: lower,
            upper: upper,
            count: self.count,
            null_count: self.null_count,
            num_bytes: self.num_bytes,
        })
    }
}

impl std::ops::BitAnd for &ColumnStatistics {
    type Output = crate::Result<ColumnStatistics>;
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
        Ok(ColumnStatistics::from_truth_value(
            nv,
            self.count.max(rhs.count),
            self.null_count.max(rhs.null_count),
            self.num_bytes.max(rhs.num_bytes),
        ))
    }
}


impl std::ops::BitOr for &ColumnStatistics {
    type Output = crate::Result<ColumnStatistics>;
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
        Ok(ColumnStatistics::from_truth_value(
            nv,
            self.count.max(rhs.count),
            self.null_count.max(rhs.null_count),
            self.num_bytes.max(rhs.num_bytes),
        ))
    }
}