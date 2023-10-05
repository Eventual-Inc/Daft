use snafu::ResultExt;

use super::ColumnStatistics;
use crate::DaftCoreComputeSnafu;

impl std::ops::Add for &ColumnStatistics {
    type Output = crate::Result<ColumnStatistics>;
    fn add(self, rhs: Self) -> Self::Output {
        Ok(ColumnStatistics {
            lower: ((&self.lower) + &rhs.lower).context(DaftCoreComputeSnafu)?,
            upper: ((&self.upper) + &rhs.upper).context(DaftCoreComputeSnafu)?,
            count: self.count.max(rhs.count),
            null_count: self.null_count.max(rhs.null_count),
            num_bytes: self.num_bytes.max(rhs.num_bytes),
        })
    }
}

impl std::ops::Sub for &ColumnStatistics {
    type Output = crate::Result<ColumnStatistics>;
    fn sub(self, rhs: Self) -> Self::Output {
        Ok(ColumnStatistics {
            lower: ((&self.lower) - &rhs.upper).context(DaftCoreComputeSnafu)?,
            upper: ((&self.upper) - &rhs.lower).context(DaftCoreComputeSnafu)?,
            count: self.count.max(rhs.count),
            null_count: self.null_count.max(rhs.null_count),
            num_bytes: self.num_bytes.max(rhs.num_bytes),
        })
    }
}
