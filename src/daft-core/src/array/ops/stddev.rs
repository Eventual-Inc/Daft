use arrow2::array::PrimitiveArray;
use common_error::DaftResult;

use crate::{
    array::{
        DataArray,
        ops::{DaftStddevAggable, GroupIndices},
    },
    datatypes::Float64Type,
    utils::stats,
};

impl DaftStddevAggable for DataArray<Float64Type> {
    type Output = DaftResult<Self>;

    fn stddev(&self) -> Self::Output {
        self.stddev_with_ddof(0)
    }

    fn stddev_with_ddof(&self, ddof: i32) -> Self::Output {
        let stats = stats::calculate_stats(self)?;
        let values = self.into_iter().flatten().copied();
        let stddev = stats::calculate_stddev_with_ddof(stats, values, ddof);
        let field = self.field.clone();
        let data = PrimitiveArray::<f64>::from([stddev]).boxed();
        Self::new(field, data)
    }

    fn grouped_stddev(&self, groups: &GroupIndices) -> Self::Output {
        self.grouped_stddev_with_ddof(groups, 0)
    }

    fn grouped_stddev_with_ddof(&self, groups: &GroupIndices, ddof: i32) -> Self::Output {
        let grouped_stddevs_iter = stats::grouped_stats(self, groups)?.map(|(stats, group)| {
            let values = group.iter().filter_map(|&index| self.get(index as _));
            stats::calculate_stddev_with_ddof(stats, values, ddof)
        });
        let field = self.field.clone();
        let data = PrimitiveArray::<f64>::from_iter(grouped_stddevs_iter).boxed();
        Self::new(field, data)
    }
}
