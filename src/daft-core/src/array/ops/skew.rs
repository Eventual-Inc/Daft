use common_error::DaftResult;

use crate::{
    array::{
        DataArray,
        ops::{DaftSkewAggable, GroupIndices},
    },
    datatypes::Float64Type,
    utils::stats,
};

impl DaftSkewAggable for DataArray<Float64Type> {
    type Output = DaftResult<Self>;

    fn skew(&self) -> Self::Output {
        let stats = stats::calculate_stats(self)?;
        let values = self.into_iter().flatten().copied();
        let skew = stats::calculate_skew(stats, values);

        Ok(Self::from_iter(self.field().clone(), std::iter::once(skew)))
    }

    fn grouped_skew(&self, groups: &GroupIndices) -> Self::Output {
        let grouped_skew_iter = stats::grouped_stats(self, groups)?.map(|(stats, group)| {
            let values = group.iter().filter_map(|&index| self.get(index as _));

            stats::calculate_skew(stats, values)
        });

        Ok(Self::from_iter(self.field().clone(), grouped_skew_iter))
    }
}
