use std::ops::Not;

use crate::DaftCoreComputeSnafu;
use daft_core::{
    array::ops::{DaftCompare, DaftLogical},
    IntoSeries,
};
use snafu::ResultExt;

use super::ColumnStatistics;

impl DaftCompare<&ColumnStatistics> for ColumnStatistics {
    type Output = crate::Result<ColumnStatistics>;
    fn equal(&self, rhs: &ColumnStatistics) -> Self::Output {
        // lower_bound: do they exactly overlap
        // upper_bound: is there any overlap
        let exactly_overlap = self
            .lower
            .equal(&rhs.lower)
            .context(DaftCoreComputeSnafu)?
            .and(&self.upper.equal(&rhs.upper).context(DaftCoreComputeSnafu)?)
            .context(DaftCoreComputeSnafu)?
            .into_series();
        let self_lower_in_rhs_bounds = self
            .lower
            .gte(&rhs.lower)
            .context(DaftCoreComputeSnafu)?
            .and(&self.lower.lte(&rhs.upper).context(DaftCoreComputeSnafu)?)
            .context(DaftCoreComputeSnafu)?;
        let rhs_lower_in_self_bounds = rhs
            .lower
            .gte(&self.lower)
            .context(DaftCoreComputeSnafu)?
            .and(&rhs.lower.lte(&self.upper).context(DaftCoreComputeSnafu)?)
            .context(DaftCoreComputeSnafu)?;
        let any_overlap = self_lower_in_rhs_bounds
            .or(&rhs_lower_in_self_bounds)
            .context(DaftCoreComputeSnafu)?
            .into_series();
        Ok(ColumnStatistics {
            lower: exactly_overlap,
            upper: any_overlap,
            count: self.count.max(rhs.count),
            null_count: self.null_count.max(rhs.null_count),
            num_bytes: self.num_bytes.max(rhs.num_bytes),
        })
    }
    fn not_equal(&self, rhs: &ColumnStatistics) -> Self::Output {
        // invert of equal
        self.equal(rhs)?.not()
    }

    fn gt(&self, rhs: &ColumnStatistics) -> Self::Output {
        // lower_bound: True greater (self.lower > rhs.upper)
        // upper_bound: some value that can be greater (self.upper > rhs.lower)
        let maybe_greater = self
            .upper
            .gt(&rhs.lower)
            .context(DaftCoreComputeSnafu)?
            .into_series();
        let always_greater = self
            .lower
            .gt(&rhs.upper)
            .context(DaftCoreComputeSnafu)?
            .into_series();
        Ok(ColumnStatistics {
            lower: always_greater,
            upper: maybe_greater,
            count: self.count.max(rhs.count),
            null_count: self.null_count.max(rhs.null_count),
            num_bytes: self.num_bytes.max(rhs.num_bytes),
        })
    }

    fn gte(&self, rhs: &ColumnStatistics) -> Self::Output {
        let maybe_gte = self
            .upper
            .gte(&rhs.lower)
            .context(DaftCoreComputeSnafu)?
            .into_series();
        let always_gte = self
            .lower
            .gte(&rhs.upper)
            .context(DaftCoreComputeSnafu)?
            .into_series();
        Ok(ColumnStatistics {
            lower: always_gte,
            upper: maybe_gte,
            count: self.count.max(rhs.count),
            null_count: self.null_count.max(rhs.null_count),
            num_bytes: self.num_bytes.max(rhs.num_bytes),
        })
    }

    fn lt(&self, rhs: &ColumnStatistics) -> Self::Output {
        // lower_bound: True less than (self.upper < rhs.lower)
        // upper_bound: some value that can be less than (self.lower < rhs.upper)
        let maybe_lt = self
            .lower
            .lt(&self.upper)
            .context(DaftCoreComputeSnafu)?
            .into_series();
        let always_lt = self
            .upper
            .lt(&self.lower)
            .context(DaftCoreComputeSnafu)?
            .into_series();
        Ok(ColumnStatistics {
            lower: always_lt,
            upper: maybe_lt,
            count: self.count.max(rhs.count),
            null_count: self.null_count.max(rhs.null_count),
            num_bytes: self.num_bytes.max(rhs.num_bytes),
        })
    }

    fn lte(&self, rhs: &ColumnStatistics) -> Self::Output {
        let maybe_lte = self
            .lower
            .lte(&self.upper)
            .context(DaftCoreComputeSnafu)?
            .into_series();
        let always_lte = self
            .upper
            .lte(&self.lower)
            .context(DaftCoreComputeSnafu)?
            .into_series();
        Ok(ColumnStatistics {
            lower: always_lte,
            upper: maybe_lte,
            count: self.count.max(rhs.count),
            null_count: self.null_count.max(rhs.null_count),
            num_bytes: self.num_bytes.max(rhs.num_bytes),
        })
    }
}
