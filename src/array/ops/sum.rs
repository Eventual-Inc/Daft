use arrow2;

use crate::{array::DataArray, datatypes::*, error::DaftResult};

use super::DaftSumAggable;

macro_rules! impl_daft_numeric_agg {
    ($T:ident) => {
        impl DaftSumAggable for &DataArray<$T> {
            type Output = DaftResult<DataArray<$T>>;

            fn sum(&self) -> Self::Output {
                let primitive_arr = self.downcast();
                let sum_value = arrow2::compute::aggregate::sum_primitive(primitive_arr);
                let arrow_array = Box::new(arrow2::array::PrimitiveArray::from([sum_value]));
                DataArray::new(self.field.clone(), arrow_array)
            }
        }
    };
}

impl_daft_numeric_agg!(Int64Type);
impl_daft_numeric_agg!(UInt64Type);
impl_daft_numeric_agg!(Float32Type);
impl_daft_numeric_agg!(Float64Type);
