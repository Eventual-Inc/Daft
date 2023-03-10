use std::sync::Arc;

use arrow2;

use crate::{array::DataArray, datatypes::*, error::DaftResult};

use super::{DaftCountAggable, DaftNumericAggable};

macro_rules! impl_daft_numeric_agg {
    ($T:ident) => {
        impl DaftNumericAggable for &DataArray<$T> {
            type SumOutput = DaftResult<DataArray<$T>>;
            type MeanOutput = DaftResult<DataArray<Float64Type>>;

            fn sum(&self) -> Self::SumOutput {
                let primitive_arr = self.downcast();

                let arrow_array = match primitive_arr.len() {
                    0 => arrow2::array::PrimitiveArray::from([]),
                    _ => {
                        let result = arrow2::compute::aggregate::sum_primitive(primitive_arr);
                        arrow2::array::PrimitiveArray::from([result])
                    }
                };
                DataArray::new(self.field.clone(), Arc::new(arrow_array))
            }

            fn mean(&self) -> Self::MeanOutput {
                let primitive_arr = self.downcast();

                let arrow_array = match primitive_arr.len() {
                    0 => arrow2::array::PrimitiveArray::from([]),
                    _ => {
                        let sum_value = Self::sum(self)?.downcast().value(0);
                        let count_value = DaftCountAggable::count(self)?.downcast().value(0);

                        let result = match count_value {
                            0 => None,
                            count_value => Some(sum_value as f64 / count_value as f64),
                        };
                        arrow2::array::PrimitiveArray::from([result])
                    }
                };

                DataArray::new(
                    Arc::new(Field::new(self.field.name.clone(), DataType::Float64)),
                    Arc::new(arrow_array),
                )
            }
        }
    };
}

impl_daft_numeric_agg!(Int64Type);
impl_daft_numeric_agg!(UInt64Type);
impl_daft_numeric_agg!(Float32Type);
impl_daft_numeric_agg!(Float64Type);
