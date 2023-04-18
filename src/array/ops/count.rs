use std::sync::Arc;

use arrow2;

use crate::{array::DataArray, datatypes::*, error::DaftResult};

use super::{DaftCountAggable, GroupIndices};

use crate::array::ops::downcast::Downcastable;

impl<T> DaftCountAggable for &DataArray<T>
where
    T: DaftDataType,
{
    type Output = DaftResult<DataArray<UInt64Type>>;

    fn count(&self) -> Self::Output {
        let arrow_array = &self.data;
        let count = arrow_array.len() - arrow_array.null_count();
        let result_arrow_array =
            Box::new(arrow2::array::PrimitiveArray::from([Some(count as u64)]));
        DataArray::<UInt64Type>::new(
            Arc::new(Field::new(self.field.name.clone(), DataType::UInt64)),
            result_arrow_array,
        )
    }
    fn grouped_count(&self, groups: &GroupIndices) -> Self::Output {
        let arrow_array = self.data.as_ref();

        let counts_per_group: Vec<_> = groups
            .iter()
            .map(|g| {
                let null_count = g
                    .downcast()
                    .values_iter()
                    .fold(0u64, |acc, v| acc + arrow_array.is_null(*v as usize) as u64);
                (g.len() as u64) - null_count
            })
            .collect();

        Ok(DataArray::<UInt64Type>::from((
            self.field.name.as_ref(),
            counts_per_group,
        )))
    }
}
