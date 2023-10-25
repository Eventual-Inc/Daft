use snafu::ResultExt;

use super::ColumnRangeStatistics;
use crate::DaftCoreComputeSnafu;

impl std::ops::Add for &ColumnRangeStatistics {
    type Output = crate::Result<ColumnRangeStatistics>;
    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (ColumnRangeStatistics::Missing, _) | (_, ColumnRangeStatistics::Missing) => {
                Ok(ColumnRangeStatistics::Missing)
            }
            (
                ColumnRangeStatistics::Loaded(s_lower, s_upper),
                ColumnRangeStatistics::Loaded(r_lower, r_upper),
            ) => Ok(ColumnRangeStatistics::Loaded(
                (s_lower + r_lower).context(DaftCoreComputeSnafu)?,
                (s_upper + r_upper).context(DaftCoreComputeSnafu)?,
            )),
        }
    }
}

impl std::ops::Sub for &ColumnRangeStatistics {
    type Output = crate::Result<ColumnRangeStatistics>;
    fn sub(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (ColumnRangeStatistics::Missing, _) | (_, ColumnRangeStatistics::Missing) => {
                Ok(ColumnRangeStatistics::Missing)
            }
            (
                ColumnRangeStatistics::Loaded(s_lower, s_upper),
                ColumnRangeStatistics::Loaded(r_lower, r_upper),
            ) => Ok(ColumnRangeStatistics::Loaded(
                (s_lower - r_upper).context(DaftCoreComputeSnafu)?,
                (s_upper - r_lower).context(DaftCoreComputeSnafu)?,
            )),
        }
    }
}
