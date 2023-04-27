use std::vec;

use arrow2::{array::Array, offset::OffsetsBuffer};

use crate::{array::BaseArray, datatypes::ListArray, error::DaftResult};

use super::{downcast::Downcastable, DaftConcatAggable};

impl DaftConcatAggable for ListArray {
    type Output = DaftResult<Self>;
    fn concat(&self) -> Self::Output {
        let array = self.downcast();
        if array.null_count() == 0 {
            let values = array.values();
            let new_offsets = OffsetsBuffer::<i64>::try_from(vec![0, values.len() as i64])?;
            let result = Box::new(arrow2::array::ListArray::<i64>::new(
                self.data_type().to_arrow()?,
                new_offsets,
                values.clone(),
                None,
            ));
            return ListArray::new(self.field.clone(), result);
        }

        let valid_arrays = array.into_iter().flatten().collect::<Vec<_>>();
        let result = arrow2::compute::concatenate::concatenate(
            valid_arrays
                .iter()
                .map(|a| a.as_ref())
                .collect::<Vec<_>>()
                .as_slice(),
        )?;
        ListArray::new(self.field.clone(), result)
    }
    fn grouped_concat(&self, _groups: &super::GroupIndices) -> Self::Output {
        todo!("impl grouped version")
    }
}
