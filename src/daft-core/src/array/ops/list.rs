use std::iter::repeat;
use std::sync::Arc;

use crate::datatypes::{Field, Int64Array, Utf8Array};
use crate::{
    array::{
        growable::{make_growable, Growable},
        FixedSizeListArray, ListArray,
    },
    datatypes::UInt64Array,
};
use crate::{CountMode, DataType};

use crate::series::{IntoSeries, Series};

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

// Given an i64 array that may have either 1 or `self.len()` elements, create an iterator with
// `self.len()` elements. If there was originally 1 element, we repeat this element `self.len()`
// times, otherwise we simply take the original array.
fn create_iter<'a>(arr: &'a Int64Array, len: usize) -> Box<dyn Iterator<Item = i64> + '_> {
    match arr.len() {
        1 => Box::new(repeat(arr.get(0).unwrap()).take(len)),
        arr_len => {
            assert_eq!(arr_len, len);
            Box::new(arr.as_arrow().iter().map(|x| *x.unwrap()))
        }
    }
}

fn get_slices_helper(
    mut parent_offsets: impl Iterator<Item = i64>,
    field: Arc<Field>,
    child_data_type: &DataType,
    flat_child: &Series,
    validity: Option<&arrow2::bitmap::Bitmap>,
    start_iter: impl Iterator<Item = i64>,
    end_iter: impl Iterator<Item = i64>,
) -> DaftResult<Series> {
    let mut slicing_indexes = Vec::with_capacity(flat_child.len());
    let mut new_offsets = Vec::with_capacity(flat_child.len() + 1);
    new_offsets.push(0);
    let mut starting_idx = parent_offsets.next().unwrap();
    for (i, ((start, end), ending_idx)) in start_iter.zip(end_iter).zip(parent_offsets).enumerate()
    {
        let is_valid = match validity {
            None => true,
            Some(v) => v.get(i).unwrap(),
        };
        let slice_start = if start >= 0 {
            starting_idx + start
        } else {
            (ending_idx + start).max(starting_idx)
        };
        let slice_end = if end >= 0 {
            (starting_idx + end).min(ending_idx)
        } else {
            ending_idx + end
        };
        let slice_length = slice_end - slice_start;
        if is_valid && slice_start >= starting_idx && slice_length > 0 {
            slicing_indexes.push(slice_start);
            new_offsets.push(new_offsets.last().unwrap() + slice_length);
        } else {
            slicing_indexes.push(-1);
            new_offsets.push(*new_offsets.last().unwrap());
        }
        starting_idx = ending_idx;
    }
    let total_capacity = *new_offsets.last().unwrap();
    let mut growable: Box<dyn Growable> = make_growable(
        &field.name,
        child_data_type,
        vec![flat_child],
        false, // We don't set validity because we can simply copy the parent's validity.
        total_capacity as usize,
    );
    for (i, start) in slicing_indexes.iter().enumerate() {
        if *start >= 0 {
            let slice_len = new_offsets.get(i + 1).unwrap() - new_offsets.get(i).unwrap();
            growable.extend(0, *start as usize, slice_len as usize);
        }
    }
    Ok(ListArray::new(
        field,
        growable.build()?,
        arrow2::offset::OffsetsBuffer::try_from(new_offsets)?,
        validity.cloned(),
    )
    .into_series())
}

fn get_chunks_helper(
    to_skip: Option<impl Iterator<Item = usize>>,
    total_elements_to_skip: usize,
    flat_child: &Series,
    field: Arc<Field>,
    new_offsets: Vec<i64>,
    validity: Option<&arrow2::bitmap::Bitmap>,
    size: usize,
) -> DaftResult<Series> {
    // If there is a list where the number of elements that do not fit properly when chunked into
    // sublists of `size` elements, we skip the elements that don't fit. If all lists in the
    // Series can be chunked cleanly, we use a fast path where we pass the underlying array of
    // elements to the result, but reinterpret it as a list of sublists. If there is at least one
    // list that cannot be chunked cleanly, we use a slower path that removes excess elements.
    if total_elements_to_skip == 0 {
        let inner_list_field = field.to_exploded_field()?.to_fixed_size_list_field(size)?;
        let inner_list = FixedSizeListArray::new(
            inner_list_field.clone(),
            Series::from_arrow(field.to_exploded_field()?.into(), flat_child.to_arrow())?,
            None,
        );
        Ok(ListArray::new(
            inner_list_field.to_list_field()?,
            inner_list.into_series(),
            arrow2::offset::OffsetsBuffer::try_from(new_offsets)?,
            validity.cloned(),
        )
        .into_series())
    } else {
        let mut growable: Box<dyn Growable> = make_growable(
            &field.name,
            &field.to_exploded_field()?.dtype,
            vec![flat_child],
            false,
            flat_child.len() - total_elements_to_skip,
        );
        let mut starting_idx = 0;
        for (i, to_skip) in to_skip.unwrap().enumerate() {
            let num_chunks = new_offsets.get(i + 1).unwrap() - new_offsets.get(i).unwrap();
            let slice_len = num_chunks as usize * size;
            growable.extend(0, starting_idx, slice_len);
            starting_idx += slice_len + to_skip;
        }
        let inner_list_field = field.to_exploded_field()?.to_fixed_size_list_field(size)?;
        let inner_list = FixedSizeListArray::new(inner_list_field.clone(), growable.build()?, None);
        Ok(ListArray::new(
            inner_list_field.to_list_field()?,
            inner_list.into_series(),
            arrow2::offset::OffsetsBuffer::try_from(new_offsets)?,
            validity.cloned(),
        )
        .into_series())
    }
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
        let idx_iter = create_iter(idx, self.len());
        self.get_children_helper(idx_iter, default)
    }

    pub fn get_slices(&self, start: &Int64Array, end: &Int64Array) -> DaftResult<Series> {
        let start_iter = create_iter(start, self.len());
        let end_iter = create_iter(end, self.len());
        get_slices_helper(
            self.offsets().iter().copied(),
            self.field.clone(),
            self.child_data_type(),
            &self.flat_child,
            self.validity(),
            start_iter,
            end_iter,
        )
    }

    pub fn get_chunks(&self, size: usize) -> DaftResult<Series> {
        let mut to_skip = Vec::with_capacity(self.flat_child.len());
        let mut new_offsets = Vec::with_capacity(self.flat_child.len() + 1);
        let mut total_elements_to_skip = 0;
        new_offsets.push(0);
        for i in 0..self.offsets().len() - 1 {
            let slice_len = self.offsets().get(i + 1).unwrap() - self.offsets().get(i).unwrap();
            let modulo = slice_len as usize % size;
            to_skip.push(modulo);
            total_elements_to_skip += modulo;
            new_offsets.push(new_offsets.last().unwrap() + (slice_len / size as i64));
        }
        let to_skip = if total_elements_to_skip == 0 {
            None
        } else {
            Some(to_skip.iter().copied())
        };
        get_chunks_helper(
            to_skip,
            total_elements_to_skip,
            &self.flat_child,
            self.field.clone(),
            new_offsets,
            self.validity(),
            size,
        )
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
        let idx_iter = create_iter(idx, self.len());
        self.get_children_helper(idx_iter, default)
    }

    pub fn get_slices(&self, start: &Int64Array, end: &Int64Array) -> DaftResult<Series> {
        let start_iter = create_iter(start, self.len());
        let end_iter = create_iter(end, self.len());
        let new_field = Arc::new(self.field.to_exploded_field()?.to_list_field()?);
        let list_size = self.fixed_element_len();
        get_slices_helper(
            (0..=((self.len() * list_size) as i64)).step_by(list_size),
            new_field,
            self.child_data_type(),
            &self.flat_child,
            self.validity(),
            start_iter,
            end_iter,
        )
    }

    pub fn get_chunks(&self, size: usize) -> DaftResult<Series> {
        let list_size = self.fixed_element_len();
        let num_chunks = list_size / size;
        let modulo = list_size % size;
        let total_elements_to_skip = modulo * self.len();
        let new_offsets: Vec<i64> = if !self.is_empty() && num_chunks > 0 {
            (0..=((self.len() * num_chunks) as i64))
                .step_by(num_chunks)
                .collect()
        } else {
            vec![0; self.len() + 1]
        };
        let to_skip = if total_elements_to_skip == 0 {
            None
        } else {
            Some(std::iter::repeat(modulo).take(self.len()))
        };
        get_chunks_helper(
            to_skip,
            total_elements_to_skip,
            &self.flat_child,
            self.field.clone(),
            new_offsets,
            self.validity(),
            size,
        )
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
