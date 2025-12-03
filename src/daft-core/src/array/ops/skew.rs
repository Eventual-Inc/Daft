use common_error::DaftResult;
use daft_arrow::array::PrimitiveArray;

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
        let field = self.field.clone();
        let data = PrimitiveArray::<f64>::from([skew]).boxed();
        Self::new(field, data)
    }

    fn grouped_skew(&self, groups: &GroupIndices) -> Self::Output {
        let grouped_skew_iter = stats::grouped_stats(self, groups)?.map(|(stats, group)| {
            let values = group.iter().filter_map(|&index| self.get(index as _));

            stats::calculate_skew(stats, values)
        });

        let field = self.field.clone();
        let data = PrimitiveArray::<f64>::from_iter(grouped_skew_iter).boxed();
        Self::new(field, data)
    }
}
