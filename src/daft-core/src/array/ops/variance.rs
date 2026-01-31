use common_error::DaftResult;
use daft_arrow::array::PrimitiveArray;

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
        let values = self.into_iter().flatten().copied();
        let variance = stats::calculate_variance(stats, values, ddof);
        let field = self.field.clone();
        let data = PrimitiveArray::<f64>::from([variance]).boxed();
        Self::new(field, data)
    }

    fn grouped_var(&self, groups: &GroupIndices, ddof: usize) -> Self::Output {
        let grouped_variances_iter = stats::grouped_stats(self, groups)?.map(|(stats, group)| {
            let values = group.iter().filter_map(|&index| self.get(index as _));
            stats::calculate_variance(stats, values, ddof)
        });
        let field = self.field.clone();
        let data = PrimitiveArray::<f64>::from_iter(grouped_variances_iter).boxed();
        Self::new(field, data)
    }
}
