use std::sync::Arc;

use crate::array::BaseArray;
use crate::datatypes::{DataType, FixedSizeListArray, ListArray, UInt64Array};

use crate::series::Series;
use crate::with_match_arrow_daft_types;
use arrow2;
use arrow2::array::Array;

use crate::error::{DaftError, DaftResult};

use super::downcast::Downcastable;

impl ListArray {
    pub fn explode(inputs: &[&Self]) -> DaftResult<(Vec<Series>, UInt64Array)> {
        assert_ne!(inputs.len(), 0, "Can only run explode on non-zero columns");
        let list_arrays = inputs.iter().map(|a| a.downcast()).collect::<Vec<_>>();

        let first_array = *list_arrays.first().unwrap();
        let first_len = first_array.len();

        if list_arrays.iter().skip(1).any(|a| a.len() != first_len) {
            return Err(DaftError::ValueError(format!(
                "Expected all arrays to be the same length in explode"
            )));
        }

        let child_arrays = list_arrays
            .iter()
            .map(|a| a.values().as_ref())
            .collect::<Vec<_>>();
        let offsets = list_arrays.iter().map(|a| a.offsets()).collect::<Vec<_>>();

        let first_offset_buffer = first_array.offsets();

        if offsets.iter().skip(1).any(|o| (*o).ne(first_offset_buffer)) {
            return Err(DaftError::ValueError(format!(
                "Expected all lists in ListArray to be the same length in explode"
            )));
        }

        let validity_arrays = list_arrays.iter().map(|a| a.validity()).collect::<Vec<_>>();
        let first_validity = first_array.validity();

        if validity_arrays
            .iter()
            .skip(1)
            .any(|o| (*o).ne(&first_validity))
        {
            return Err(DaftError::ValueError(format!(
                "Expected validity in ListArray to be the same for all columns in explode"
            )));
        }

        let total_capacity: i64 = (0..first_len)
            .map(|i| {
                let is_valid = first_array.is_valid(i);
                let len: i64 =
                    first_offset_buffer.get(i + 1).unwrap() - first_offset_buffer.get(i).unwrap();
                match (is_valid, len) {
                    (false, _) => 1,
                    (true, 0) => 1,
                    (true, l) => l,
                }
            })
            .sum();

        let mut growables = child_arrays
            .iter()
            .map(|c| arrow2::array::growable::make_growable(&[*c], true, total_capacity as usize))
            .collect::<Vec<_>>();
        let mut indices = Vec::with_capacity(total_capacity as usize);

        for i in 0..first_len {
            let is_valid = first_array.is_valid(i);
            let start = first_offset_buffer.get(i).unwrap();
            let len = first_offset_buffer.get(i + 1).unwrap() - start;
            match (is_valid, len) {
                (false, _) => {
                    growables.iter_mut().for_each(|g| g.extend_validity(1));
                    indices.push(i as u64);
                }
                (true, 0) => {
                    growables.iter_mut().for_each(|g| g.extend_validity(1));
                    indices.push(i as u64);
                }
                (true, l) => {
                    growables
                        .iter_mut()
                        .for_each(|g| g.extend(0, *start as usize, l as usize));
                    (0..l).for_each(|_| indices.push(i as u64));
                }
            }
        }

        let idx_to_take = UInt64Array::from(("idx_to_take", indices));
        let mut series_to_return = Vec::with_capacity(inputs.len());

        for (a, g) in inputs.iter().zip(growables.iter()) {
            let child_data_type = match a.data_type() {
                DataType::List(field) => &field.dtype,
                _ => panic!("Expected List type but received {:?}", a.data_type()),
            };

            with_match_arrow_daft_types!(child_data_type,|$T| {
                let new_data_arr = DataArray::<$T>::new(Arc::new(Field {
                    name: a.field.name.clone(),
                    dtype: child_data_type.clone(),
                }), g.as_box())?;
                series_to_return.push(new_data_arr.into_series());
            })
        }

        Ok((series_to_return, idx_to_take))
    }
}

impl FixedSizeListArray {
    pub fn explode(&self) -> DaftResult<(Series, UInt64Array)> {
        let list_array = self.downcast();
        let child_array = list_array.values().as_ref();

        let list_size = list_array.size();

        let mut total_capacity: i64 =
            (list_size * (list_array.len() - list_array.null_count())) as i64;

        if list_size == 0 {
            total_capacity = list_array.len() as i64;
        }

        let mut growable =
            arrow2::array::growable::make_growable(&[child_array], true, total_capacity as usize);
        let mut indices = Vec::with_capacity(total_capacity as usize);

        for i in 0..list_array.len() {
            let is_valid = list_array.is_valid(i) && (list_size > 0);
            match is_valid {
                false => {
                    growable.extend_validity(1);
                    indices.push(i as u64);
                }
                true => {
                    let start = i * list_size;
                    growable.extend(0, start, list_size);
                    (0..list_size).for_each(|_| indices.push(i as u64));
                }
            }
        }

        let child_data_type = match self.data_type() {
            DataType::FixedSizeList(field, _) => &field.dtype,
            _ => panic!(
                "Expected FixedSizeList type but received {:?}",
                self.data_type()
            ),
        };

        let idx_to_take = UInt64Array::from(("idx_to_take", indices));
        with_match_arrow_daft_types!(child_data_type,|$T| {
            let new_data_arr = DataArray::<$T>::new(Arc::new(Field {
                name: self.field.name.clone(),
                dtype: child_data_type.clone(),
            }), growable.as_box())?;
            Ok((new_data_arr.into_series(), idx_to_take))
        })
    }
}
