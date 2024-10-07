use std::sync::Arc;

use arrow2::array::PrimitiveArray;
use common_error::DaftResult;
use daft_schema::{dtype::DataType, field::Field};

use crate::{
    array::{
        ops::{DaftStddevAggable, GroupIndices},
        DataArray,
    },
    datatypes::Float64Type,
    utils::stats,
};

impl DaftStddevAggable for DataArray<Float64Type> {
    type Output = DaftResult<Self>;

    fn stddev(&self) -> Self::Output {
        let stats = stats::calculate_stats(self)?;
        let values = self.into_iter().flatten().copied();
        let stddev = stats::calculate_stddev(stats, values);
        let field = Arc::new(Field::new(self.field.name.clone(), DataType::Float64));
        let data = PrimitiveArray::<f64>::from([stddev]).boxed();
        Self::new(field, data)
    }

    fn grouped_stddev(&self, groups: &GroupIndices) -> Self::Output {
        let grouped_stddevs_iter = stats::grouped_stats(self, groups)?.map(|(stats, group)| {
            let values = group.iter().filter_map(|&index| self.get(index as _));
            stats::calculate_stddev(stats, values)
        });
        let field = Arc::new(Field::new(self.field.name.clone(), DataType::Float64));
        let data = PrimitiveArray::<f64>::from_iter(grouped_stddevs_iter).boxed();
        Self::new(field, data)
    }
}
