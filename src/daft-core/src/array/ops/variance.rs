use common_error::DaftResult;

use crate::{
    array::{
        DataArray,
        ops::{DaftVarianceAggable, GroupIndices},
    },
    datatypes::Float64Type,
    utils::stats,
};

impl DaftVarianceAggable for DataArray<Float64Type> {
    type Output = DaftResult<Self>;

    fn var(&self, ddof: usize) -> Self::Output {
        let stats = stats::calculate_stats(self)?;
        let values = self.into_iter().flatten();
        let variance = stats::calculate_variance(stats, values, ddof);
        Ok(Self::from_iter(
            self.field().clone(),
            std::iter::once(variance),
        ))
    }

    fn grouped_var(&self, groups: &GroupIndices, ddof: usize) -> Self::Output {
        let grouped_variances_iter = stats::grouped_stats(self, groups)?.map(|(stats, group)| {
            let values = group.iter().filter_map(|&index| self.get(index as _));
            stats::calculate_variance(stats, values, ddof)
        });
        Ok(Self::from_iter(
            self.field().clone(),
            grouped_variances_iter,
        ))
    }
}
