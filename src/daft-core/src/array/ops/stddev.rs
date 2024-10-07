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
    utils::stats::{stats, Stats},
};

impl DaftStddevAggable for DataArray<Float64Type> {
    type Output = DaftResult<Self>;

    fn stddev(&self) -> Self::Output {
        let Stats { count, mean, .. } = stats(self)?;
        let stddev = mean.map(|mean| {
            let mut square_sum = 0.0;
            for &value in self.into_iter().flatten() {
                square_sum += (value - mean).powi(2);
            }
            let variance = square_sum / count as f64;
            variance.sqrt()
        });
        let field = Arc::new(Field::new(self.field.name.clone(), DataType::Float64));
        let data = PrimitiveArray::<f64>::from([stddev]).boxed();
        Self::new(field, data)
    }

    fn grouped_stddev(&self, _: &GroupIndices) -> Self::Output {
        todo!("stddev")
    }
}
