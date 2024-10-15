use std::sync::Arc;

use arrow2::array::PrimitiveArray;
use common_error::DaftResult;

use crate::{
    array::ops::{DaftMeanAggable, GroupIndices},
    datatypes::*,
    utils::stats,
};

impl DaftMeanAggable for DataArray<Float64Type> {
    type Output = DaftResult<Self>;

    fn mean(&self) -> Self::Output {
        let stats = stats::calculate_stats(self)?;
        let data = PrimitiveArray::from([stats.mean]).boxed();
        let field = Arc::new(Field::new(self.field.name.clone(), DataType::Float64));
        Self::new(field, data)
    }

    fn grouped_mean(&self, groups: &GroupIndices) -> Self::Output {
        let grouped_means = stats::grouped_stats(self, groups)?.map(|(stats, _)| stats.mean);
        let data = Box::new(PrimitiveArray::from_iter(grouped_means));
        Ok(Self::from((self.field.name.as_ref(), data)))
    }
}
