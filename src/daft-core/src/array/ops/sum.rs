use common_error::DaftResult;

use super::{DaftSumAggable, as_arrow::AsArrow};
use crate::{array::ops::GroupIndices, datatypes::*};
macro_rules! impl_daft_numeric_agg {
    ($T:ident, $AggType: ident) => {
        impl DaftSumAggable for &DataArray<$T> {
            type Output = DaftResult<DataArray<$T>>;

            fn sum(&self) -> Self::Output {
                let arrow_array = self.as_arrow()?;
                let sum_value = arrow::compute::sum(&arrow_array);
                Ok(DataArray::<$T>::from_iter(
                    self.field.clone(),
                    std::iter::once(sum_value),
                ))
            }

            fn grouped_sum(&self, groups: &GroupIndices) -> Self::Output {
                let sum_per_group = if self.null_count() > 0 {
                    DataArray::<$T>::from_iter(
                        self.field.clone(),
                        groups.iter().map(|g| {
                            g.iter().fold(None, |acc, index| {
                                let idx = *index as usize;
                                match (acc, self.get(idx)) {
                                    (acc, None) => acc,
                                    (None, Some(val)) => Some(val),
                                    (Some(acc), Some(val)) => Some(acc + val),
                                }
                            })
                        }),
                    )
                } else {
                    DataArray::<$T>::from_values_iter(
                        self.field.clone(),
                        groups.iter().map(|g| {
                            g.iter().fold(0 as $AggType, |acc, index| {
                                let idx = *index as usize;
                                acc + self.get(idx).unwrap()
                            })
                        }),
                    )
                };

                Ok(sum_per_group)
            }
        }
    };
}

impl_daft_numeric_agg!(Int64Type, i64);
impl_daft_numeric_agg!(UInt64Type, u64);
impl_daft_numeric_agg!(Float32Type, f32);
impl_daft_numeric_agg!(Float64Type, f64);
impl_daft_numeric_agg!(Decimal128Type, i128);
