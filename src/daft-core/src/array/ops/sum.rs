use arrow2;

use super::DaftSumAggable;

use super::as_arrow::AsArrow;

use crate::array::ops::GroupIndices;
use crate::{array::DataArray, datatypes::*};

use common_error::DaftResult;

use arrow2::array::Array;
macro_rules! impl_daft_numeric_agg {
    ($T:ident, $AggType: ident) => {
        impl DaftSumAggable for &DataArray<$T> {
            type Output = DaftResult<DataArray<$T>>;

            fn sum(&self) -> Self::Output {
                let primitive_arr = self.as_arrow();
                let sum_value = arrow2::compute::aggregate::sum_primitive(primitive_arr);
                let arrow_array = Box::new(arrow2::array::PrimitiveArray::from([sum_value]));
                DataArray::new(self.field.clone(), arrow_array)
            }

            fn grouped_sum(&self, groups: &GroupIndices) -> Self::Output {
                use arrow2::array::PrimitiveArray;
                let arrow_array = self.as_arrow();
                let sum_per_group = if arrow_array.null_count() > 0 {
                    Box::new(PrimitiveArray::from_trusted_len_iter(groups.iter().map(
                        |g| {
                            g.iter().fold(None, |acc, index| {
                                let idx = *index as usize;
                                match (acc, arrow_array.is_null(idx)) {
                                    (acc, true) => acc,
                                    (None, false) => Some(arrow_array.value(idx)),
                                    (Some(acc), false) => Some(acc + arrow_array.value(idx)),
                                }
                            })
                        },
                    )))
                } else {
                    Box::new(PrimitiveArray::from_trusted_len_values_iter(
                        groups.iter().map(|g| {
                            g.iter().fold(0 as $AggType, |acc, index| {
                                let idx = *index as usize;
                                acc + unsafe { arrow_array.value_unchecked(idx) }
                            })
                        }),
                    ))
                };

                Ok(DataArray::from((self.field.name.as_ref(), sum_per_group)))
            }
        }
    };
}

impl_daft_numeric_agg!(Int64Type, i64);
impl_daft_numeric_agg!(UInt64Type, u64);
impl_daft_numeric_agg!(Float32Type, f32);
impl_daft_numeric_agg!(Float64Type, f64);
