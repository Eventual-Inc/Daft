use std::iter::repeat;
use std::ops::Add;

use crate::array::DataArray;
use crate::array::{
    growable::{make_growable, Growable},
    FixedSizeListArray, ListArray,
};
use crate::datatypes::{DaftNumericType, Float64Array, Int64Array, UInt64Array, Utf8Array};
use crate::{CountMode, DataType};

use crate::series::Series;

use arrow2;

use arrow2::array::PrimitiveArray;
use arrow2::compute::aggregate::Sum;
use common_error::{DaftError, DaftResult};

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

trait ListChildAggable {
    fn agg_data_array<T, U>(&self, arr: &DataArray<T>, op: U) -> DaftResult<Series>
    where
        T: DaftNumericType,
        <T::Native as arrow2::types::simd::Simd>::Simd:
            Add<Output = <T::Native as arrow2::types::simd::Simd>::Simd> + Sum<T::Native>,
        U: Fn(&PrimitiveArray<T::Native>) -> Option<T::Native>;

    fn _min(&self, flat_child: &Series) -> DaftResult<Series> {
        use crate::datatypes::DataType::*;
        use arrow2::compute::aggregate::min_primitive;

        match flat_child.data_type() {
            Int8 => self.agg_data_array(flat_child.i8()?, &min_primitive),
            Int16 => self.agg_data_array(flat_child.i16()?, &min_primitive),
            Int32 => self.agg_data_array(flat_child.i32()?, &min_primitive),
            Int64 => self.agg_data_array(flat_child.i64()?, &min_primitive),
            UInt8 => self.agg_data_array(flat_child.u8()?, &min_primitive),
            UInt16 => self.agg_data_array(flat_child.u16()?, &min_primitive),
            UInt32 => self.agg_data_array(flat_child.u32()?, &min_primitive),
            UInt64 => self.agg_data_array(flat_child.u64()?, &min_primitive),
            Float32 => self.agg_data_array(flat_child.f32()?, &min_primitive),
            Float64 => self.agg_data_array(flat_child.f64()?, &min_primitive),
            other => Err(DaftError::TypeError(format!(
                "Min not implemented for {}",
                other
            ))),
        }
    }

    fn _max(&self, flat_child: &Series) -> DaftResult<Series> {
        use crate::datatypes::DataType::*;
        use arrow2::compute::aggregate::max_primitive;

        match flat_child.data_type() {
            Int8 => self.agg_data_array(flat_child.i8()?, &max_primitive),
            Int16 => self.agg_data_array(flat_child.i16()?, &max_primitive),
            Int32 => self.agg_data_array(flat_child.i32()?, &max_primitive),
            Int64 => self.agg_data_array(flat_child.i64()?, &max_primitive),
            UInt8 => self.agg_data_array(flat_child.u8()?, &max_primitive),
            UInt16 => self.agg_data_array(flat_child.u16()?, &max_primitive),
            UInt32 => self.agg_data_array(flat_child.u32()?, &max_primitive),
            UInt64 => self.agg_data_array(flat_child.u64()?, &max_primitive),
            Float32 => self.agg_data_array(flat_child.f32()?, &max_primitive),
            Float64 => self.agg_data_array(flat_child.f64()?, &max_primitive),
            other => Err(DaftError::TypeError(format!(
                "Max not implemented for {}",
                other
            ))),
        }
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

    pub fn sum(&self) -> DaftResult<Series> {
        use crate::datatypes::DataType::*;
        use arrow2::compute::aggregate::sum_primitive;

        match self.flat_child.data_type() {
            Int8 | Int16 | Int32 | Int64 => {
                let casted = self.flat_child.cast(&Int64)?;
                let arr = casted.i64()?;

                self.agg_data_array(arr, &sum_primitive)
            }
            UInt8 | UInt16 | UInt32 | UInt64 => {
                let casted = self.flat_child.cast(&UInt64)?;
                let arr = casted.u64()?;

                self.agg_data_array(arr, &sum_primitive)
            }
            Float32 => self.agg_data_array(self.flat_child.f32()?, &sum_primitive),
            Float64 => self.agg_data_array(self.flat_child.f64()?, &sum_primitive),
            other => Err(DaftError::TypeError(format!(
                "Sum not implemented for {}",
                other
            ))),
        }
    }

    pub fn mean(&self) -> DaftResult<Float64Array> {
        use crate::datatypes::DataType::*;

        match self.flat_child.data_type() {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32 | Float64 => {
                let counts = self.count(CountMode::Valid)?;
                let sum_series = self.sum()?.cast(&Float64)?;
                let sums = sum_series.f64()?;

                let means = counts
                    .into_iter()
                    .zip(sums)
                    .map(|(count, sum)| match (count, sum) {
                        (Some(count), Some(sum)) if *count != 0 => Some(sum / (*count as f64)),
                        _ => None,
                    });

                let arr = Box::new(arrow2::array::PrimitiveArray::from_trusted_len_iter(means));

                Ok(Float64Array::from((self.name(), arr)))
            }
            other => Err(DaftError::TypeError(format!(
                "Mean not implemented for {}",
                other
            ))),
        }
    }

    pub fn min(&self) -> DaftResult<Series> {
        self._min(&self.flat_child)
    }

    pub fn max(&self) -> DaftResult<Series> {
        self._max(&self.flat_child)
    }
}

impl ListChildAggable for ListArray {
    fn agg_data_array<T, U>(&self, arr: &DataArray<T>, op: U) -> DaftResult<Series>
    where
        T: DaftNumericType,
        <T::Native as arrow2::types::simd::Simd>::Simd:
            Add<Output = <T::Native as arrow2::types::simd::Simd>::Simd> + Sum<T::Native>,
        U: Fn(&PrimitiveArray<T::Native>) -> Option<T::Native>,
    {
        let aggs = arrow2::types::IndexRange::new(0, self.len() as u64)
            .map(|i| i as usize)
            .map(|i| {
                if let Some(validity) = self.validity() && !validity.get_bit(i) {
                    return None;
                }

                let start = *self.offsets().get(i).unwrap() as usize;
                let end = *self.offsets().get(i + 1).unwrap() as usize;

                let slice = arr.slice(start, end).unwrap();
                let slice_arr = slice.as_arrow();

                op(slice_arr)
            });

        let array = arrow2::array::PrimitiveArray::from_trusted_len_iter(aggs).boxed();

        Series::try_from((self.name(), array))
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

    fn agg_data_array<T, U>(&self, arr: &DataArray<T>, op: U) -> DaftResult<Series>
    where
        T: DaftNumericType,
        <T::Native as arrow2::types::simd::Simd>::Simd:
            Add<Output = <T::Native as arrow2::types::simd::Simd>::Simd> + Sum<T::Native>,
        U: Fn(&PrimitiveArray<T::Native>) -> Option<T::Native>,
    {
        let step = self.fixed_element_len();
        let aggs = arrow2::types::IndexRange::new(0, self.len() as u64)
            .map(|i| i as usize)
            .map(|i| {
                if let Some(validity) = self.validity() && !validity.get_bit(i) {
                    return None;
                }

                let start = i * step;
                let end = (i + 1) * step;

                let slice = arr.slice(start, end).unwrap();
                let slice_arr = slice.as_arrow();

                op(slice_arr)
            });

        let array = arrow2::array::PrimitiveArray::from_trusted_len_iter(aggs).boxed();

        Series::try_from((self.name(), array))
    }

    pub fn sum(&self) -> DaftResult<Series> {
        use crate::datatypes::DataType::*;
        use arrow2::compute::aggregate::sum_primitive;

        match self.flat_child.data_type() {
            Int8 | Int16 | Int32 | Int64 => {
                let casted = self.flat_child.cast(&Int64)?;
                let arr = casted.i64()?;

                self.agg_data_array(arr, &sum_primitive)
            }
            UInt8 | UInt16 | UInt32 | UInt64 => {
                let casted = self.flat_child.cast(&UInt64)?;
                let arr = casted.u64()?;

                self.agg_data_array(arr, &sum_primitive)
            }
            Float32 => self.agg_data_array(self.flat_child.f32()?, &sum_primitive),
            Float64 => self.agg_data_array(self.flat_child.f64()?, &sum_primitive),
            other => Err(DaftError::TypeError(format!(
                "Sum not implemented for {}",
                other
            ))),
        }
    }

    pub fn mean(&self) -> DaftResult<Float64Array> {
        use crate::datatypes::DataType::*;

        match self.flat_child.data_type() {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32 | Float64 => {
                let counts = self.count(CountMode::Valid)?;
                let sum_series = self.sum()?.cast(&Float64)?;
                let sums = sum_series.f64()?;

                let means = counts
                    .into_iter()
                    .zip(sums)
                    .map(|(count, sum)| match (count, sum) {
                        (Some(count), Some(sum)) if *count != 0 => Some(sum / (*count as f64)),
                        _ => None,
                    });

                let arr = Box::new(arrow2::array::PrimitiveArray::from_trusted_len_iter(means));

                Ok(Float64Array::from((self.name(), arr)))
            }
            other => Err(DaftError::TypeError(format!(
                "Mean not implemented for {}",
                other
            ))),
        }
    }

    pub fn min(&self) -> DaftResult<Series> {
        self._min(&self.flat_child)
    }

    pub fn max(&self) -> DaftResult<Series> {
        self._max(&self.flat_child)
    }
}

impl ListChildAggable for FixedSizeListArray {
    fn agg_data_array<T, U>(&self, arr: &DataArray<T>, op: U) -> DaftResult<Series>
    where
        T: DaftNumericType,
        <T::Native as arrow2::types::simd::Simd>::Simd:
            Add<Output = <T::Native as arrow2::types::simd::Simd>::Simd> + Sum<T::Native>,
        U: Fn(&PrimitiveArray<T::Native>) -> Option<T::Native>,
    {
        let step = self.fixed_element_len();
        let aggs = arrow2::types::IndexRange::new(0, self.len() as u64)
            .map(|i| i as usize)
            .map(|i| {
                if let Some(validity) = self.validity() && !validity.get_bit(i) {
                    return None;
                }

                let start = i * step;
                let end = (i + 1) * step;

                let slice = arr.slice(start, end).unwrap();
                let slice_arr = slice.as_arrow();

                op(slice_arr)
            });

        let array = arrow2::array::PrimitiveArray::from_trusted_len_iter(aggs).boxed();

        Series::try_from((self.name(), array))
    }
}
