use std::sync::Arc;

use arrow2::array::PrimitiveArray;
use common_error::DaftResult;

use crate::{
    array::ops::{
        as_arrow::AsArrow, DaftCountAggable, DaftMeanAggable, DaftSumAggable, GroupIndices,
    },
    count_mode::CountMode,
    datatypes::*,
    utils::stats,
};

impl DaftMeanAggable for DataArray<Float64Type> {
    type Output = DaftResult<Self>;

    fn mean(&self) -> Self::Output {
        let stats = stats::calculate_stats(self)?;
        let mean = stats::calculate_mean(stats.sum, stats.count);
        let data = PrimitiveArray::from([mean]).boxed();
        let field = Arc::new(Field::new(self.field.name.clone(), DataType::Float64));
        Self::new(field, data)
    }

    fn grouped_mean(&self, groups: &GroupIndices) -> Self::Output {
        let sum_values = self.grouped_sum(groups)?;
        let count_values = self.grouped_count(groups, CountMode::Valid)?;
        assert_eq!(sum_values.len(), count_values.len());
        let mean_per_group = sum_values
            .as_arrow()
            .values_iter()
            .zip(count_values.as_arrow().values_iter())
            .map(|(s, c)| match (s, c) {
                (_, 0) => None,
                (s, c) => Some(s / (*c as f64)),
            });
        let mean_array = Box::new(PrimitiveArray::from_trusted_len_iter(mean_per_group));
        Ok(Self::from((self.field.name.as_ref(), mean_array)))
    }
}
