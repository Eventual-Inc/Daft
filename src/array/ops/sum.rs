use std::sync::Arc;

use arrow2;

use crate::{array::DataArray, datatypes::*, error::DaftResult};

use super::DaftNumericAgg;

macro_rules! impl_daft_numeric_agg {
    ($arrayT:ident) => {
        impl DaftNumericAgg for &$arrayT {
            type Output = DaftResult<$arrayT>;

            fn sum(&self) -> Self::Output {
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
        }
    };
}

impl_daft_numeric_agg!(Int64Array);
impl_daft_numeric_agg!(UInt64Array);
impl_daft_numeric_agg!(Float32Array);
impl_daft_numeric_agg!(Float64Array);
