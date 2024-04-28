use std::iter::repeat;

use crate::datatypes::{Int64Array, Utf8Array};
use crate::{
    array::{
        growable::{make_growable, Growable},
        FixedSizeListArray, ListArray,
    },
    datatypes::UInt64Array,
};
use crate::{CountMode, DataType};

use crate::series::Series;

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
    pub fn count(&self, mode: CountMode) -> DaftResult<UInt64Array> {
        let counts = match (mode, self.flat_child.validity()) {
            (CountMode::All, _) | (CountMode::Valid, None) => {
                self.offsets().lengths().map(|l| l as u64).collect()
            }
            (CountMode::Valid, Some(validity)) => self
                .offsets()
                .windows(2)
                .map(|w| {
                    (w[0]..w[1])
                        .map(|i| validity.get_bit(i as usize) as u64)
                        .sum()
                })
                .collect(),
            (CountMode::Null, None) => repeat(0).take(self.offsets().len() - 1).collect(),
            (CountMode::Null, Some(validity)) => self
                .offsets()
                .windows(2)
                .map(|w| {
                    (w[0]..w[1])
                        .map(|i| !validity.get_bit(i as usize) as u64)
                        .sum()
                })
                .collect(),
        };

        let array = Box::new(
            arrow2::array::PrimitiveArray::from_vec(counts).with_validity(self.validity().cloned()),
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
        idx_iter: impl Iterator<Item = i64>,
        default: &Series,
    ) -> DaftResult<Series> {
        assert!(
            default.len() == 1,
            "Only a single default value is supported"
        );
        let default = default.cast(self.child_data_type())?;

        let mut growable = make_growable(
            self.name(),
            self.child_data_type(),
            vec![&self.flat_child, &default],
            true,
            self.len(),
        );

        let offsets = self.offsets();

        for (i, child_idx) in idx_iter.enumerate() {
            let is_valid = self.is_valid(i);
            let start = *offsets.get(i).unwrap();
            let end = *offsets.get(i + 1).unwrap();

            let idx_offset = if child_idx >= 0 {
                start + child_idx
            } else {
                end + child_idx
            };

            if is_valid && idx_offset >= start && idx_offset < end {
                growable.extend(0, idx_offset as usize, 1);
            } else {
                // uses the default value in the case where the row is invalid or the index is out of bounds
                growable.extend(1, 0, 1);
            }
        }

        growable.build()
    }

    pub fn get_children(&self, idx: &Int64Array, default: &Series) -> DaftResult<Series> {
        match idx.len() {
            1 => {
                let idx_iter = repeat(idx.get(0).unwrap()).take(self.len());
                self.get_children_helper(idx_iter, default)
            }
            len => {
                assert_eq!(len, self.len());
                let idx_iter = idx.as_arrow().iter().map(|x| *x.unwrap());
                self.get_children_helper(idx_iter, default)
            }
        }
    }
}

impl FixedSizeListArray {
    pub fn count(&self, mode: CountMode) -> DaftResult<UInt64Array> {
        let size = self.fixed_element_len();
        let counts = match (mode, self.flat_child.validity()) {
            (CountMode::All, _) | (CountMode::Valid, None) => {
                repeat(size as u64).take(self.len()).collect()
            }
            (CountMode::Valid, Some(validity)) => (0..self.len())
                .map(|i| {
                    (0..size)
                        .map(|j| validity.get_bit(i * size + j) as u64)
                        .sum()
                })
                .collect(),
            (CountMode::Null, None) => repeat(0).take(self.len()).collect(),
            (CountMode::Null, Some(validity)) => (0..self.len())
                .map(|i| {
                    (0..size)
                        .map(|j| !validity.get_bit(i * size + j) as u64)
                        .sum()
                })
                .collect(),
        };

        let array = Box::new(
            arrow2::array::PrimitiveArray::from_vec(counts).with_validity(self.validity().cloned()),
        );
        Ok(UInt64Array::from((self.name(), array)))
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
        idx_iter: impl Iterator<Item = i64>,
        default: &Series,
    ) -> DaftResult<Series> {
        assert!(
            default.len() == 1,
            "Only a single default value is supported"
        );
        let default = default.cast(self.child_data_type())?;

        let mut growable = make_growable(
            self.name(),
            self.child_data_type(),
            vec![&self.flat_child, &default],
            true,
            self.len(),
        );

        let list_size = self.fixed_element_len();

        for (i, child_idx) in idx_iter.enumerate() {
            let is_valid = self.is_valid(i);

            if is_valid && child_idx.abs() < list_size as i64 {
                let idx_offset = if child_idx >= 0 {
                    (i * list_size) as i64 + child_idx
                } else {
                    ((i + 1) * list_size) as i64 + child_idx
                };

                growable.extend(0, idx_offset as usize, 1);
            } else {
                // uses the default value in the case where the row is invalid or the index is out of bounds
                growable.extend(1, 0, 1);
            }
        }

        growable.build()
    }

    pub fn get_children(&self, idx: &Int64Array, default: &Series) -> DaftResult<Series> {
        match idx.len() {
            1 => {
                let idx_iter = repeat(idx.get(0).unwrap()).take(self.len());
                self.get_children_helper(idx_iter, default)
            }
            len => {
                assert_eq!(len, self.len());
                let idx_iter = idx.as_arrow().iter().map(|x| *x.unwrap());
                self.get_children_helper(idx_iter, default)
            }
        }
    }
}

macro_rules! impl_aggs_list_array {
    ($la:ident) => {
        impl $la {
            fn agg_helper<T>(&self, op: T) -> DaftResult<Series>
            where
                T: Fn(&Series) -> DaftResult<Series>,
            {
                // TODO(Kevin): Currently this requires full materialization of one Series for every list. We could avoid this by implementing either sorted aggregation or an array builder

                // Assumes `op`` returns a null Series given an empty Series
                let aggs = self
                    .into_iter()
                    .map(|s| s.unwrap_or(Series::empty("", self.child_data_type())))
                    .map(|s| op(&s))
                    .collect::<DaftResult<Vec<_>>>()?;

                let agg_refs: Vec<_> = aggs.iter().collect();

                Series::concat(agg_refs.as_slice()).map(|s| s.rename(self.name()))
            }

            pub fn sum(&self) -> DaftResult<Series> {
                self.agg_helper(|s| s.sum(None))
            }

            pub fn mean(&self) -> DaftResult<Series> {
                self.agg_helper(|s| s.mean(None))
            }

            pub fn min(&self) -> DaftResult<Series> {
                self.agg_helper(|s| s.min(None))
            }

            pub fn max(&self) -> DaftResult<Series> {
                self.agg_helper(|s| s.max(None))
            }
        }
    };
}

impl_aggs_list_array!(ListArray);
impl_aggs_list_array!(FixedSizeListArray);
