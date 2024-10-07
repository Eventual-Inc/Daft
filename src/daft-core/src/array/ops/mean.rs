use std::sync::Arc;

use arrow2::array::PrimitiveArray;
use common_error::DaftResult;

use super::{as_arrow::AsArrow, DaftCountAggable, DaftMeanAggable, DaftSumAggable};
use crate::{
    array::ops::GroupIndices,
    count_mode::CountMode,
    datatypes::*,
    utils::stats::{stats, Stats},
};
impl DaftMeanAggable for &DataArray<Float64Type> {
    type Output = DaftResult<DataArray<Float64Type>>;

    fn mean(&self) -> Self::Output {
        let Stats { mean, count, .. } = stats(self)?;
        let value = mean.map(|mean| mean / count as f64);
        let data = PrimitiveArray::from([value]).boxed();
        let field = Arc::new(Field::new(self.field.name.clone(), DataType::Float64));
        DataArray::new(field, data)
    }

    fn grouped_mean(&self, groups: &GroupIndices) -> Self::Output {
        use arrow2::array::PrimitiveArray;
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
        Ok(DataArray::from((self.field.name.as_ref(), mean_array)))
    }
}
