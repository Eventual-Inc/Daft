use snafu::ResultExt;

use super::ColumnRangeStatistics;
use crate::DaftCoreComputeSnafu;

impl std::ops::Add for &ColumnRangeStatistics {
    type Output = crate::Result<ColumnRangeStatistics>;
    fn add(self, rhs: Self) -> Self::Output {
        Ok(ColumnRangeStatistics {
            lower: ((&self.lower) + &rhs.lower).context(DaftCoreComputeSnafu)?,
            upper: ((&self.upper) + &rhs.upper).context(DaftCoreComputeSnafu)?,
        })
    }
}

impl std::ops::Sub for &ColumnRangeStatistics {
    type Output = crate::Result<ColumnRangeStatistics>;
    fn sub(self, rhs: Self) -> Self::Output {
        Ok(ColumnRangeStatistics {
            lower: ((&self.lower) - &rhs.upper).context(DaftCoreComputeSnafu)?,
            upper: ((&self.upper) - &rhs.lower).context(DaftCoreComputeSnafu)?,
        })
    }
}
