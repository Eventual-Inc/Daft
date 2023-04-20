use std::sync::Arc;

use crate::datatypes::{Field, ListArray, UInt64Array};
use crate::series::Series;
use crate::with_match_daft_types;
use crate::{
    array::{
        nested::{ExplodableArray, NestedArray},
        BaseArray,
    },
    datatypes::FixedSizeListArray,
};
use arrow2;

use crate::error::DaftResult;

use super::downcast::Downcastable;

impl ExplodableArray for ListArray {
    fn explode(&self) -> DaftResult<(Series, UInt64Array)> {
        let arr = self.downcast();
        let child_type = self.child_data_type();
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

        // Use lengths of the collected children to calculate the indices to repeat other columns by
        let idx_to_take = UInt64Array::from((
            "repeat_idx",
            collected_children_view
                .iter()
                .map(|x| x.len() as u64)
                .enumerate()
                .flat_map(|(row_idx, num_row_repeats)| {
                    std::iter::repeat(row_idx as u64).take(num_row_repeats as usize)
                })
                .collect::<Vec<u64>>(),
        ));

        with_match_daft_types!(child_type, |$T| {
            let new_data_arr = DataArray::<$T>::new(child_field, new_arr)?;
            Ok((new_data_arr.into_series(), idx_to_take))
        })
    }
}

impl ExplodableArray for FixedSizeListArray {
    fn explode(&self) -> DaftResult<(Series, UInt64Array)> {
        let arr = self.downcast();
        let child_type = self.child_data_type();
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

        // Use lengths of the collected children to calculate the indices to repeat other columns by
        let idx_to_take = UInt64Array::from((
            "repeat_idx",
            collected_children_view
                .iter()
                .map(|x| x.len() as u64)
                .enumerate()
                .flat_map(|(row_idx, num_row_repeats)| {
                    std::iter::repeat(row_idx as u64).take(num_row_repeats as usize)
                })
                .collect::<Vec<u64>>(),
        ));

        with_match_daft_types!(child_type, |$T| {
            let new_data_arr = DataArray::<$T>::new(child_field, new_arr)?;
            Ok((new_data_arr.into_series(), idx_to_take))
        })
    }
}
