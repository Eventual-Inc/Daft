use std::sync::Arc;

use crate::array::BaseArray;
use crate::datatypes::{DataType, FixedSizeListArray, ListArray, UInt64Array};
use crate::series::Series;
use crate::with_match_daft_types;
use arrow2;

use crate::error::DaftResult;

use super::downcast::Downcastable;

// Receives the elements of an explodable arrow2 array (e.g. a ListArray) as an iterator of arrow2 Arrays
// Explodes these elements into a flattened Array, returning both this Array as well as a vector of indices indicating
// the row index that each element of the flattened Array corresponds to
fn explode_arrow_array_iter(
    child_type: &arrow2::datatypes::DataType,
    iterator: impl Iterator<Item = Option<Box<dyn arrow2::array::Array>>>,
) -> DaftResult<(Box<dyn arrow2::array::Array>, Vec<u64>)> {
    let mut collected_children: Vec<Box<dyn arrow2::array::Array>> = Vec::new();
    for child in iterator {
        match child {
            None => collected_children
                .push(arrow2::array::new_null_array(child_type.clone(), 1).to_boxed()),
            Some(child_chunk) if child_chunk.len() == 0 => collected_children
                .push(arrow2::array::new_null_array(child_type.clone(), 1).to_boxed()),
            Some(child_chunk) => {
                collected_children.push(child_chunk);
            }
        }
    }
    let collected_children_view = collected_children
        .iter()
        .map(|x| x.as_ref())
        .collect::<Vec<_>>();
    let new_arr = arrow2::compute::concatenate::concatenate(collected_children_view.as_slice())?;

    // Use lengths of the collected children to calculate the indices to repeat other columns by
    let idx_to_take = collected_children_view
        .iter()
        .map(|x| x.len() as u64)
        .enumerate()
        .flat_map(|(row_idx, num_row_repeats)| {
            std::iter::repeat(row_idx as u64).take(num_row_repeats as usize)
        })
        .collect::<Vec<u64>>();

    Ok((new_arr, idx_to_take))
}

impl ListArray {
    pub fn explode(&self) -> DaftResult<(Series, UInt64Array)> {
        let arr = self.downcast();
        let child_data_type = match self.data_type() {
            DataType::List(field) => &field.dtype,
            _ => panic!("Expected List type but received {:?}", self.data_type()),
        };
        let (exploded_arr, indices) =
            explode_arrow_array_iter(&child_data_type.to_arrow()?, arr.iter())?;
        with_match_daft_types!(child_data_type, |$T| {
            let new_data_arr = DataArray::<$T>::new(Arc::new(Field {
                name: self.field.name.clone(),
                dtype: child_data_type.clone(),
            }), exploded_arr)?;
            let idx_to_take = UInt64Array::from(("idx_to_take", indices));
            Ok((new_data_arr.into_series(), idx_to_take))
        })
    }
}

impl FixedSizeListArray {
    pub fn explode(&self) -> DaftResult<(Series, UInt64Array)> {
        let arr = self.downcast();
        let child_data_type = match self.data_type() {
            DataType::FixedSizeList(field, _) => &field.dtype,
            _ => panic!(
                "Expected FixedSizeList type but received {:?}",
                self.data_type()
            ),
        };
        let (exploded_arr, indices) =
            explode_arrow_array_iter(&child_data_type.to_arrow()?, arr.iter())?;
        with_match_daft_types!(child_data_type, |$T| {
            let new_data_arr = DataArray::<$T>::new(Arc::new(Field {
                name: self.field.name.clone(),
                dtype: child_data_type.clone(),
            }), exploded_arr)?;
            let idx_to_take = UInt64Array::from(("idx_to_take", indices));
            Ok((new_data_arr.into_series(), idx_to_take))
        })
    }
}
