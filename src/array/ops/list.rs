use std::sync::Arc;

use crate::array::{BaseArray, DataArray};
use crate::datatypes::{DaftListLikeType, DataType, FixedSizeListArray, ListArray, UInt64Array};
use crate::dsl::functions::list;
use crate::series::Series;
use crate::{with_match_arrow_daft_types, with_match_daft_types};
use arrow2;
use arrow2::array::{growable, Array};

use crate::error::DaftResult;

use super::downcast::Downcastable;

impl ListArray {
    pub fn explode(&self) -> DaftResult<(Series, UInt64Array)> {
        let list_array = self.downcast();
        let child_array = list_array.values().as_ref();
        let offsets = list_array.offsets();

        let total_capacity: i64 = (0..list_array.len())
            .map(|i| {
                let is_valid = list_array.is_valid(i);
                let len: i64 = offsets.get(i + 1).unwrap() - offsets.get(i).unwrap();
                match (is_valid, len) {
                    (false, _) => 1,
                    (true, 0) => 1,
                    (true, l) => l,
                }
            })
            .sum();
        let mut growable =
            arrow2::array::growable::make_growable(&[child_array], true, total_capacity as usize);
        let mut indices = Vec::with_capacity(total_capacity as usize);

        for i in 0..list_array.len() {
            let is_valid = list_array.is_valid(i);
            let start = offsets.get(i).unwrap();
            let len = offsets.get(i + 1).unwrap() - start;
            match (is_valid, len) {
                (false, _) => {
                    growable.extend_validity(1);
                    indices.push(i as u64);
                }
                (true, 0) => {
                    growable.extend_validity(1);
                    indices.push(i as u64);
                }
                (true, l) => {
                    growable.extend(0, *start as usize, l as usize);
                    (0..l).for_each(|_| indices.push(i as u64));
                }
            }
        }

        let child_data_type = match self.data_type() {
            DataType::List(field) => &field.dtype,
            _ => panic!("Expected List type but received {:?}", self.data_type()),
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
