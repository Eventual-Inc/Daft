use std::sync::Arc;

use daft_arrow::types::months_days_ns;

use super::as_arrow::AsArrow;
#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    array::{DataArray, FixedSizeListArray, ListArray},
    datatypes::{
        BinaryArray, BooleanArray, DaftLogicalType, DaftPrimitiveType, ExtensionArray, FileArray,
        FixedSizeBinaryArray, IntervalArray, NullArray, Utf8Array,
        logical::{
            DateArray, DurationArray, LogicalArrayImpl, MapArray, TimeArray, TimestampArray,
        },
    },
    file::{DaftMediaType, FileReference},
    series::Series,
};

impl<T> DataArray<T>
where
    T: DaftPrimitiveType,
{
    #[inline]
    pub fn get(&self, idx: usize) -> Option<T::Native> {
        assert!(
            idx < self.len(),
            "Out of bounds: {} vs len: {}",
            idx,
            self.len()
        );
        let arrow_array = self.as_arrow2();
        let is_valid = arrow_array
            .validity()
            .is_none_or(|validity| validity.get_bit(idx));
        if is_valid {
            Some(unsafe { arrow_array.value_unchecked(idx) })
        } else {
            None
        }
    }
}

// Default implementations of get ops for DataArray and LogicalArray.
macro_rules! impl_array_arrow_get {
    ($ArrayT:ty, $output:ty) => {
        impl $ArrayT {
            #[inline]
            pub fn get(&self, idx: usize) -> Option<$output> {
                assert!(
                    idx < self.len(),
                    "Out of bounds: {} vs len: {}",
                    idx,
                    self.len()
                );

                let arrow_array = self.as_arrow2();
                let is_valid = arrow_array
                    .validity()
                    .is_none_or(|validity| validity.get_bit(idx));
                if is_valid {
                    Some(unsafe { arrow_array.value_unchecked(idx) })
                } else {
                    None
                }
            }
        }
    };
}

impl<L: DaftLogicalType> LogicalArrayImpl<L, FixedSizeListArray> {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<Series> {
        self.physical.get(idx)
    }
}

impl_array_arrow_get!(Utf8Array, &str);
impl_array_arrow_get!(BooleanArray, bool);
impl_array_arrow_get!(BinaryArray, &[u8]);
impl_array_arrow_get!(FixedSizeBinaryArray, &[u8]);
impl_array_arrow_get!(DateArray, i32);
impl_array_arrow_get!(TimeArray, i64);
impl_array_arrow_get!(DurationArray, i64);
impl_array_arrow_get!(IntervalArray, months_days_ns);
impl_array_arrow_get!(TimestampArray, i64);

impl NullArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<()> {
        assert!(
            idx < self.len(),
            "Out of bounds: {} vs len: {}",
            idx,
            self.len()
        );
        None
    }
}

impl ExtensionArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<Box<dyn daft_arrow::scalar::Scalar>> {
        assert!(
            idx < self.len(),
            "Out of bounds: {} vs len: {}",
            idx,
            self.len()
        );
        let is_valid = self
            .data
            .validity()
            .is_none_or(|validity| validity.get_bit(idx));
        if is_valid {
            Some(daft_arrow::scalar::new_scalar(self.data(), idx))
        } else {
            None
        }
    }
}

#[cfg(feature = "python")]
impl PythonArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<Arc<pyo3::Py<pyo3::PyAny>>> {
        assert!(
            idx < self.len(),
            "Out of bounds: {} vs len: {}",
            idx,
            self.len()
        );
        if self.validity().is_none_or(|v| v.is_valid(idx)) {
            self.values().get(idx).cloned()
        } else {
            None
        }
    }
}

impl FixedSizeListArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<Series> {
        assert!(
            idx < self.len(),
            "Out of bounds: {} vs len: {}",
            idx,
            self.len()
        );
        let fixed_len = self.fixed_element_len();
        let valid = self.is_valid(idx);
        if valid {
            Some(
                self.flat_child
                    .slice(idx * fixed_len, (idx + 1) * fixed_len)
                    .unwrap(),
            )
        } else {
            None
        }
    }
}

impl ListArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<Series> {
        assert!(
            idx < self.len(),
            "Out of bounds: {} vs len: {}",
            idx,
            self.len()
        );
        let valid = self.is_valid(idx);
        if valid {
            let (start, end) = self.offsets().start_end(idx);
            Some(self.flat_child.slice(start, end).unwrap())
        } else {
            None
        }
    }
}

impl MapArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<Series> {
        self.physical.get(idx)
    }
}

impl<T> FileArray<T>
where
    T: DaftMediaType,
{
    #[inline]
    pub fn get(&self, idx: usize) -> Option<FileReference> {
        let url_array = self.physical.get("url").expect("url exists");
        let io_config_array = self.physical.get("io_config").expect("io_config exists");
        let url_array = url_array.utf8().expect("url is utf8");
        let io_config_array = io_config_array.binary().expect("io_config is binary");

        let data = url_array.get(idx)?;
        let io_config = io_config_array.get(idx);
        let io_config: Option<common_io_config::IOConfig> = {
            io_config
                .map(|serialized| {
                    bincode::serde::decode_from_slice::<common_io_config::IOConfig, _>(
                        serialized,
                        bincode::config::legacy(),
                    )
                    .map(|out| out.0)
                })
                .transpose()
                .ok()
                .flatten()
        };

        Some(FileReference::new(
            T::get_type(),
            data.to_string(),
            io_config,
        ))
    }
}

#[cfg(test)]
mod tests {
    use common_error::DaftResult;

    use crate::{
        array::FixedSizeListArray,
        datatypes::{DataType, Field, Int32Array},
        series::IntoSeries,
    };

    #[test]
    fn test_fixed_size_list_get_all_valid() -> DaftResult<()> {
        let field = Field::new("foo", DataType::FixedSizeList(Box::new(DataType::Int32), 3));
        let flat_child = Int32Array::from(("foo", (0..9).collect::<Vec<i32>>()));
        let validity = None;
        let arr = FixedSizeListArray::new(field, flat_child.into_series(), validity);
        assert_eq!(arr.len(), 3);

        for i in 0..3 {
            let element = arr.get(i);
            assert!(element.is_some());

            let element = element.unwrap();
            assert_eq!(element.len(), 3);
            assert_eq!(element.data_type(), &DataType::Int32);

            let element = element.i32()?;
            let data = element
                .into_iter()
                .map(|x| x.copied())
                .collect::<Vec<Option<i32>>>();
            let expected = ((i * 3) as i32..((i + 1) * 3) as i32)
                .map(Some)
                .collect::<Vec<Option<i32>>>();
            assert_eq!(data, expected);
        }

        Ok(())
    }

    #[test]
    fn test_fixed_size_list_get_some_valid() -> DaftResult<()> {
        let field = Field::new("foo", DataType::FixedSizeList(Box::new(DataType::Int32), 3));
        let flat_child = Int32Array::from(("foo", (0..9).collect::<Vec<i32>>()));
        let raw_validity = vec![true, false, true];
        let validity = Some(daft_arrow::buffer::NullBuffer::from(
            raw_validity.as_slice(),
        ));
        let arr = FixedSizeListArray::new(field, flat_child.into_series(), validity);
        assert_eq!(arr.len(), 3);

        let element = arr.get(0);
        assert!(element.is_some());
        let element = element.unwrap();
        assert_eq!(element.len(), 3);
        assert_eq!(element.data_type(), &DataType::Int32);
        let element = element.i32()?;
        let data = element
            .into_iter()
            .map(|x| x.copied())
            .collect::<Vec<Option<i32>>>();
        let expected = vec![Some(0), Some(1), Some(2)];
        assert_eq!(data, expected);

        let element = arr.get(1);
        assert!(element.is_none());

        let element = arr.get(2);
        assert!(element.is_some());
        let element = element.unwrap();
        assert_eq!(element.len(), 3);
        assert_eq!(element.data_type(), &DataType::Int32);
        let element = element.i32()?;
        let data = element
            .into_iter()
            .map(|x| x.copied())
            .collect::<Vec<Option<i32>>>();
        let expected = vec![Some(6), Some(7), Some(8)];
        assert_eq!(data, expected);

        Ok(())
    }

    #[test]
    fn test_list_get_some_valid() -> DaftResult<()> {
        let field = Field::new("foo", DataType::FixedSizeList(Box::new(DataType::Int32), 3));
        let flat_child = Int32Array::from(("foo", (0..9).collect::<Vec<i32>>()));
        let raw_validity = vec![true, false, true];
        let validity = Some(daft_arrow::buffer::NullBuffer::from(
            raw_validity.as_slice(),
        ));
        let arr = FixedSizeListArray::new(field, flat_child.into_series(), validity);
        let list_dtype = DataType::List(Box::new(DataType::Int32));
        let list_arr = arr.cast(&list_dtype)?;
        let l = list_arr.list()?;
        let element = l.get(0).unwrap();
        let element = element.i32()?;
        let data = element
            .into_iter()
            .map(|x| x.copied())
            .collect::<Vec<Option<i32>>>();
        let expected = vec![Some(0), Some(1), Some(2)];
        assert_eq!(data, expected);
        let element = l.get(2).unwrap();
        let element = element.i32()?;
        let data = element
            .into_iter()
            .map(|x| x.copied())
            .collect::<Vec<Option<i32>>>();
        let expected = vec![Some(6), Some(7), Some(8)];
        assert_eq!(data, expected);

        Ok(())
    }
}
