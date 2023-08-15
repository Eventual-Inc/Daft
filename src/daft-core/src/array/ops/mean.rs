use std::sync::Arc;

use arrow2;

use crate::count_mode::CountMode;
use crate::{array::DataArray, datatypes::*};

use common_error::DaftResult;

use super::{DaftCountAggable, DaftMeanAggable, DaftSumAggable};

use super::as_arrow::AsArrow;

use crate::array::ops::GroupIndices;
impl DaftMeanAggable for &DataArray<Float64Type> {
    type Output = DaftResult<DataArray<Float64Type>>;

    fn mean(&self) -> Self::Output {
        let sum_value = DaftSumAggable::sum(self)?.as_arrow().value(0);
        let count_value = DaftCountAggable::count(self, CountMode::Valid)?
            .as_arrow()
            .value(0);

        let result = match count_value {
            0 => None,
            count_value => Some(sum_value / count_value as f64),
        };
        let arrow_array = Box::new(arrow2::array::PrimitiveArray::from([result]));

        DataArray::new(
            Arc::new(Field::new(self.field.name.clone(), DataType::Float64)),
            arrow_array,
        )
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
