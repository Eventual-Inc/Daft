use std::ops::Not;

use daft_core::prelude::*;
use snafu::ResultExt;

use super::ColumnRangeStatistics;
use crate::DaftCoreComputeSnafu;

impl DaftCompare<&Self> for ColumnRangeStatistics {
    type Output = crate::Result<Self>;
    fn equal(&self, rhs: &Self) -> Self::Output {
        // lower_bound: do they exactly overlap
        // upper_bound: is there any overlap

        match (self, rhs) {
            (Self::Missing, _) | (_, Self::Missing) => Ok(Self::Missing),
            (Self::Loaded(s_lower, s_upper), Self::Loaded(r_lower, r_upper)) => {
                let exactly_overlap = (s_lower.equal(r_lower).context(DaftCoreComputeSnafu)?)
                    .and(&s_upper.equal(r_upper).context(DaftCoreComputeSnafu)?)
                    .context(DaftCoreComputeSnafu)?
                    .into_series();

                let self_lower_in_rhs_bounds = s_lower
                    .gte(r_lower)
                    .context(DaftCoreComputeSnafu)?
                    .and(&s_lower.lte(r_upper).context(DaftCoreComputeSnafu)?)
                    .context(DaftCoreComputeSnafu)?;
                let rhs_lower_in_self_bounds = r_lower
                    .gte(s_lower)
                    .context(DaftCoreComputeSnafu)?
                    .and(&r_lower.lte(s_upper).context(DaftCoreComputeSnafu)?)
                    .context(DaftCoreComputeSnafu)?;

                let any_overlap = self_lower_in_rhs_bounds
                    .or(&rhs_lower_in_self_bounds)
                    .context(DaftCoreComputeSnafu)?
                    .into_series();
                Ok(Self::Loaded(exactly_overlap, any_overlap))
            }
        }
    }
    fn not_equal(&self, rhs: &Self) -> Self::Output {
        // invert of equal
        self.equal(rhs)?.not()
    }

    fn eq_null_safe(&self, rhs: &Self) -> Self::Output {
        self.equal(rhs)
    }

    fn gt(&self, rhs: &Self) -> Self::Output {
        // lower_bound: True greater (self.lower > rhs.upper)
        // upper_bound: some value that can be greater (self.upper > rhs.lower)

        match (self, rhs) {
            (Self::Missing, _) | (_, Self::Missing) => Ok(Self::Missing),
            (Self::Loaded(s_lower, s_upper), Self::Loaded(r_lower, r_upper)) => {
                let maybe_greater = s_upper
                    .gt(r_lower)
                    .context(DaftCoreComputeSnafu)?
                    .into_series();
                let always_greater = s_lower
                    .gt(r_upper)
                    .context(DaftCoreComputeSnafu)?
                    .into_series();
                Ok(Self::Loaded(always_greater, maybe_greater))
            }
        }
    }

    fn gte(&self, rhs: &Self) -> Self::Output {
        match (self, rhs) {
            (Self::Missing, _) | (_, Self::Missing) => Ok(Self::Missing),
            (Self::Loaded(s_lower, s_upper), Self::Loaded(r_lower, r_upper)) => {
                let maybe_gte = s_upper
                    .gte(r_lower)
                    .context(DaftCoreComputeSnafu)?
                    .into_series();
                let always_gte = s_lower
                    .gte(r_upper)
                    .context(DaftCoreComputeSnafu)?
                    .into_series();
                Ok(Self::Loaded(always_gte, maybe_gte))
            }
        }
    }

    fn lt(&self, rhs: &Self) -> Self::Output {
        // lower_bound: True less than (self.upper < rhs.lower)
        // upper_bound: some value that can be less than (self.lower < rhs.upper)

        match (self, rhs) {
            (Self::Missing, _) | (_, Self::Missing) => Ok(Self::Missing),
            (Self::Loaded(s_lower, s_upper), Self::Loaded(r_lower, r_upper)) => {
                let maybe_lt = s_lower
                    .lt(r_upper)
                    .context(DaftCoreComputeSnafu)?
                    .into_series();
                let always_lt = s_upper
                    .lt(r_lower)
                    .context(DaftCoreComputeSnafu)?
                    .into_series();
                Ok(Self::Loaded(always_lt, maybe_lt))
            }
        }
    }

    fn lte(&self, rhs: &Self) -> Self::Output {
        match (self, rhs) {
            (Self::Missing, _) | (_, Self::Missing) => Ok(Self::Missing),
            (Self::Loaded(s_lower, s_upper), Self::Loaded(r_lower, r_upper)) => {
                let maybe_lte = s_lower
                    .lte(r_upper)
                    .context(DaftCoreComputeSnafu)?
                    .into_series();
                let always_lte = s_upper
                    .lte(r_lower)
                    .context(DaftCoreComputeSnafu)?
                    .into_series();
                Ok(Self::Loaded(always_lte, maybe_lte))
            }
        }
    }
}

impl ColumnRangeStatistics {
    pub fn union(&self, rhs: &Self) -> crate::Result<Self> {
        match (self, rhs) {
            (Self::Missing, _) | (_, Self::Missing) => Ok(Self::Missing),
            (Self::Loaded(s_lower, s_upper), Self::Loaded(r_lower, r_upper)) => {
                let new_min = s_lower.if_else(
                    r_lower,
                    &(s_lower.lt(r_lower))
                        .context(DaftCoreComputeSnafu)?
                        .into_series(),
                );
                let new_max = s_upper.if_else(
                    r_upper,
                    &(s_upper.gt(r_upper))
                        .context(DaftCoreComputeSnafu)?
                        .into_series(),
                );

                Ok(Self::Loaded(
                    new_min.context(DaftCoreComputeSnafu)?,
                    new_max.context(DaftCoreComputeSnafu)?,
                ))
            }
        }
    }
}
