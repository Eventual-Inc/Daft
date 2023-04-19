use std::sync::Arc;

use crate::array::{BaseArray, DataArray};
use crate::datatypes::{DaftDataType, DataType, Field, Int64Array, ListArray};
use arrow2;

use crate::error::DaftResult;

use super::downcast::Downcastable;

impl ListArray {
    fn child_type(&self) -> DataType {
        match self.data_type() {
            DataType::List(field) => field.dtype.clone(),
            DataType::FixedSizeList(field, _) => field.dtype.clone(),
            _ => panic!("Expected list or fixed size list"),
        }
    }

    pub fn explode<T: DaftDataType>(&self) -> DaftResult<(DataArray<T>, Int64Array)> {
        let arr = self.downcast();
        let child_type = self.child_type();
        let child_field = Arc::new(Field {
            name: self.field.name.clone(),
            dtype: child_type.clone(),
        });

        // Explode the list column:
        //   1. Elements which are null are replaced with a null value
        //   2. Elements with len=0 are replaced with a null value
        //   3. Elements with len>0 will have their contents flattened into the new array
        let mut collected_children: Vec<Box<dyn arrow2::array::Array>> = Vec::new();
        for child in arr.iter() {
            match child {
                None => collected_children
                    .push(arrow2::array::new_null_array(child_type.to_arrow()?, 1).to_boxed()),
                Some(child_chunk) if child_chunk.len() == 0 => collected_children
                    .push(arrow2::array::new_null_array(child_type.to_arrow()?, 1).to_boxed()),
                Some(child_chunk) => {
                    collected_children.push(child_chunk);
                }
            }
        }
        let collected_children_view = collected_children
            .iter()
            .map(|x| x.as_ref())
            .collect::<Vec<_>>();
        let new_arr =
            arrow2::compute::concatenate::concatenate(collected_children_view.as_slice())?;
        let new_data_arr = DataArray::new(child_field, new_arr)?;

        // Use lengths of the collected children to calculate the indices to repeat other columns by
        let idx_to_take = Int64Array::from((
            "repeat_idx",
            collected_children_view
                .iter()
                .map(|x| x.len() as i64)
                .enumerate()
                .flat_map(|(row_idx, num_row_repeats)| {
                    std::iter::repeat(row_idx as i64).take(num_row_repeats as usize)
                })
                .collect::<Vec<i64>>(),
        ));

        Ok((new_data_arr, idx_to_take))
    }
}
