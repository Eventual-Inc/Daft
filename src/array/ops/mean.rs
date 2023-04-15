use std::sync::Arc;

use arrow2;

use crate::{array::DataArray, datatypes::*, error::DaftResult};

use super::{DaftCountAggable, DaftMeanAggable, DaftSumAggable};

use super::downcast::Downcastable;

impl DaftMeanAggable for &DataArray<Float64Type> {
    type Output = DaftResult<DataArray<Float64Type>>;

    fn mean(&self) -> Self::Output {
        let sum_value = DaftSumAggable::sum(self)?.downcast().value(0);
        let count_value = DaftCountAggable::count(self)?.downcast().value(0);

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
}
