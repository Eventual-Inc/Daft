use std::sync::Arc;

use crate::array::BaseArray;
use crate::datatypes::{DataType, FixedSizeListArray, ListArray, UInt64Array};

use crate::series::Series;
use crate::with_match_arrow_daft_types;
use arrow2;
use arrow2::array::Array;

use crate::error::DaftResult;

use super::downcast::Downcastable;

impl ListArray {
    pub fn lengths(&self) -> DaftResult<UInt64Array> {
        let list_array = self.downcast();
        let offsets = list_array.offsets();

        let mut lens = Vec::with_capacity(self.len());
        for i in 0..self.len() {
            lens.push((unsafe { offsets.get_unchecked(i + 1) - offsets.get_unchecked(i) }) as u64)
        }
        let array = Box::new(
            arrow2::array::PrimitiveArray::from_vec(lens)
                .with_validity(list_array.validity().cloned()),
        );
        Ok(UInt64Array::from((self.name(), array)))
    }

    pub fn explode(&self) -> DaftResult<Series> {
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

        for i in 0..list_array.len() {
            let is_valid = list_array.is_valid(i);
            let start = offsets.get(i).unwrap();
            let len = offsets.get(i + 1).unwrap() - start;
            match (is_valid, len) {
                (false, _) => growable.extend_validity(1),
                (true, 0) => growable.extend_validity(1),
                (true, l) => growable.extend(0, *start as usize, l as usize),
            }
        }

        let child_data_type = match self.data_type() {
            DataType::List(field) => &field.dtype,
            _ => panic!("Expected List type but received {:?}", self.data_type()),
        };

        with_match_arrow_daft_types!(child_data_type,|$T| {
            let new_data_arr = DataArray::<$T>::new(Arc::new(Field {
                name: self.field.name.clone(),
                dtype: child_data_type.clone(),
            }), growable.as_box())?;
            Ok(new_data_arr.into_series())
        })
    }
}

impl FixedSizeListArray {
    pub fn lengths(&self) -> DaftResult<UInt64Array> {
        let list_array = self.downcast();
        let list_size = list_array.size();
        let lens = (0..self.len())
            .map(|_| list_size as u64)
            .collect::<Vec<_>>();
        let array = Box::new(
            arrow2::array::PrimitiveArray::from_vec(lens)
                .with_validity(list_array.validity().cloned()),
        );
        Ok(UInt64Array::from((self.name(), array)))
    }

    pub fn explode(&self) -> DaftResult<Series> {
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

        for i in 0..list_array.len() {
            let is_valid = list_array.is_valid(i) && (list_size > 0);
            match is_valid {
                false => growable.extend_validity(1),
                true => growable.extend(0, i * list_size, list_size),
            }
        }

        let child_data_type = match self.data_type() {
            DataType::FixedSizeList(field, _) => &field.dtype,
            _ => panic!(
                "Expected FixedSizeList type but received {:?}",
                self.data_type()
            ),
        };

        with_match_arrow_daft_types!(child_data_type,|$T| {
            let new_data_arr = DataArray::<$T>::new(Arc::new(Field {
                name: self.field.name.clone(),
                dtype: child_data_type.clone(),
            }), growable.as_box())?;
            Ok(new_data_arr.into_series())
        })
    }
}
