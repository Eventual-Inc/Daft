use std::iter::repeat;

use crate::array::{
    growable::{make_growable, Growable},
    FixedSizeListArray, ListArray,
};
use crate::datatypes::{Int64Array, UInt64Array, Utf8Array};
use crate::DataType;

use crate::series::Series;

use arrow2;

use common_error::DaftResult;

use super::as_arrow::AsArrow;

fn join_arrow_list_of_utf8s(
    list_element: Option<&dyn arrow2::array::Array>,
    delimiter_str: &str,
) -> Option<String> {
    list_element
        .map(|list_element| {
            list_element
                .as_any()
                .downcast_ref::<arrow2::array::Utf8Array<i64>>()
                .unwrap()
                .iter()
                .fold(String::from(""), |acc, str_item| {
                    acc + str_item.unwrap_or("") + delimiter_str
                })
            // Remove trailing `delimiter_str`
        })
        .map(|result| {
            let result_len = result.len();
            if result_len > 0 {
                result[..result_len - delimiter_str.len()].to_string()
            } else {
                result
            }
        })
}

impl ListArray {
    pub fn lengths(&self) -> DaftResult<UInt64Array> {
        let lengths = self.offsets().lengths().map(|l| Some(l as u64));
        let array = Box::new(
            arrow2::array::PrimitiveArray::from_iter(lengths)
                .with_validity(self.validity().cloned()),
        );
        Ok(UInt64Array::from((self.name(), array)))
    }

    pub fn explode(&self) -> DaftResult<Series> {
        let offsets = self.offsets();

        let total_capacity: usize = (0..self.len())
            .map(|i| {
                let is_valid = self.is_valid(i);
                let len: usize = (offsets.get(i + 1).unwrap() - offsets.get(i).unwrap()) as usize;
                match (is_valid, len) {
                    (false, _) => 1,
                    (true, 0) => 1,
                    (true, l) => l,
                }
            })
            .sum();
        let mut growable: Box<dyn Growable> = make_growable(
            self.name(),
            self.child_data_type(),
            vec![&self.flat_child],
            true,
            total_capacity,
        );

        for i in 0..self.len() {
            let is_valid = self.is_valid(i);
            let start = offsets.get(i).unwrap();
            let len = offsets.get(i + 1).unwrap() - start;
            match (is_valid, len) {
                (false, _) => growable.add_nulls(1),
                (true, 0) => growable.add_nulls(1),
                (true, l) => growable.extend(0, *start as usize, l as usize),
            }
        }

        growable.build()
    }

    pub fn join(&self, delimiter: &Utf8Array) -> DaftResult<Utf8Array> {
        assert_eq!(self.child_data_type(), &DataType::Utf8,);

        let delimiter_iter: Box<dyn Iterator<Item = Option<&str>>> = if delimiter.len() == 1 {
            Box::new(repeat(delimiter.get(0)).take(self.len()))
        } else {
            assert_eq!(delimiter.len(), self.len());
            Box::new(delimiter.as_arrow().iter())
        };
        let self_iter = (0..self.len()).map(|i| self.get(i));

        let result = self_iter
            .zip(delimiter_iter)
            .map(|(list_element, delimiter)| {
                join_arrow_list_of_utf8s(
                    list_element.as_ref().map(|l| l.utf8().unwrap().data()),
                    delimiter.unwrap_or(""),
                )
            });

        Ok(Utf8Array::from((
            self.name(),
            Box::new(arrow2::array::Utf8Array::from_iter(result)),
        )))
    }

    fn get_children_helper(
        &self,
        idx_iter: &mut impl Iterator<Item = Option<i64>>,
    ) -> DaftResult<Series> {
        let mut growable = make_growable(
            self.name(),
            self.child_data_type(),
            vec![&self.flat_child],
            true,
            self.len(),
        );

        let offsets = self.offsets();

        for i in 0..self.len() {
            let is_valid = self.is_valid(i);
            let start = *offsets.get(i).unwrap();
            let end = *offsets.get(i + 1).unwrap();
            let child_idx = idx_iter.next().unwrap().unwrap();

            // only add index value when list is valid and index is within bounds
            match (is_valid, child_idx >= 0, start + child_idx, end + child_idx) {
                (true, true, idx_offset, _) if idx_offset < end => {
                    growable.extend(0, idx_offset as usize, 1)
                }
                (true, false, _, idx_offset) if idx_offset >= start => {
                    growable.extend(0, idx_offset as usize, 1)
                }
                _ => growable.add_nulls(1),
            };
        }

        growable.build()
    }

    pub fn get_children(&self, idx: &Int64Array) -> DaftResult<Series> {
        match idx.len() {
            1 => {
                let mut idx_iter = repeat(idx.get(0)).take(self.len());
                self.get_children_helper(&mut idx_iter)
            }
            _ => {
                assert_eq!(idx.len(), self.len());
                let mut idx_iter = idx.as_arrow().iter().map(|x| x.copied());
                self.get_children_helper(&mut idx_iter)
            }
        }
    }
}

impl FixedSizeListArray {
    pub fn lengths(&self) -> DaftResult<UInt64Array> {
        let size = self.fixed_element_len();
        match self.validity() {
            None => Ok(UInt64Array::from((
                self.name(),
                repeat(size as u64)
                    .take(self.len())
                    .collect::<Vec<_>>()
                    .as_slice(),
            ))),
            Some(validity) => {
                let arrow_arr = arrow2::array::UInt64Array::from_iter(validity.iter().map(|v| {
                    if v {
                        Some(size as u64)
                    } else {
                        None
                    }
                }));
                Ok(UInt64Array::from((self.name(), Box::new(arrow_arr))))
            }
        }
    }

    pub fn explode(&self) -> DaftResult<Series> {
        let list_size = self.fixed_element_len();
        let total_capacity = if list_size == 0 {
            self.len()
        } else {
            let null_count = self.validity().map(|v| v.unset_bits()).unwrap_or(0);
            list_size * (self.len() - null_count)
        };

        let mut child_growable: Box<dyn Growable> = make_growable(
            self.name(),
            self.child_data_type(),
            vec![&self.flat_child],
            true,
            total_capacity,
        );

        for i in 0..self.len() {
            let is_valid = self.is_valid(i) && (list_size > 0);
            match is_valid {
                false => child_growable.add_nulls(1),
                true => child_growable.extend(0, i * list_size, list_size),
            }
        }
        child_growable.build()
    }

    pub fn join(&self, delimiter: &Utf8Array) -> DaftResult<Utf8Array> {
        assert_eq!(self.child_data_type(), &DataType::Utf8,);

        let delimiter_iter: Box<dyn Iterator<Item = Option<&str>>> = if delimiter.len() == 1 {
            Box::new(repeat(delimiter.get(0)).take(self.len()))
        } else {
            assert_eq!(delimiter.len(), self.len());
            Box::new(delimiter.as_arrow().iter())
        };
        let self_iter = (0..self.len()).map(|i| self.get(i));

        let result = self_iter
            .zip(delimiter_iter)
            .map(|(list_element, delimiter)| {
                join_arrow_list_of_utf8s(
                    list_element.as_ref().map(|l| l.utf8().unwrap().data()),
                    delimiter.unwrap_or(""),
                )
            });

        Ok(Utf8Array::from((
            self.name(),
            Box::new(arrow2::array::Utf8Array::from_iter(result)),
        )))
    }

    fn get_children_helper(
        &self,
        idx_iter: &mut impl Iterator<Item = Option<i64>>,
    ) -> DaftResult<Series> {
        let mut growable = make_growable(
            self.name(),
            self.child_data_type(),
            vec![&self.flat_child],
            true,
            self.len(),
        );

        let list_size = self.fixed_element_len();

        for i in 0..self.len() {
            let is_valid = self.is_valid(i);
            let child_idx = idx_iter.next().unwrap().unwrap();

            // only add index value when list is valid and index is within bounds
            match (is_valid, child_idx.abs() < list_size as i64, child_idx >= 0) {
                (true, true, true) => growable.extend(0, i * list_size + child_idx as usize, 1),
                (true, true, false) => {
                    growable.extend(0, (i + 1) * list_size + child_idx as usize, 1)
                }
                _ => growable.add_nulls(1),
            };
        }

        growable.build()
    }

    pub fn get_children(&self, idx: &Int64Array) -> DaftResult<Series> {
        match idx.len() {
            1 => {
                let mut idx_iter = repeat(idx.get(0)).take(self.len());
                self.get_children_helper(&mut idx_iter)
            }
            _ => {
                assert_eq!(idx.len(), self.len());
                let mut idx_iter = idx.as_arrow().iter().map(|x| x.copied());
                self.get_children_helper(&mut idx_iter)
            }
        }
    }
}
