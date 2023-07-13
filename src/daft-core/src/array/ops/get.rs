use crate::{
    array::DataArray,
    datatypes::{
        logical::{
            DateArray, Decimal128Array, DurationArray, EmbeddingArray, FixedShapeImageArray,
            ImageArray, TimestampArray,
        },
        BinaryArray, BooleanArray, DaftNumericType, ExtensionArray, FixedSizeListArray, ListArray,
        NullArray, StructArray, Utf8Array,
    },
};

use super::as_arrow::AsArrow;

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    #[inline]
    pub fn get(&self, idx: usize) -> Option<T::Native> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        let arrow_array = self.as_arrow();
        let is_valid = arrow_array
            .validity()
            .map_or(true, |validity| validity.get_bit(idx));
        if is_valid {
            Some(unsafe { arrow_array.value_unchecked(idx) })
        } else {
            None
        }
    }
}

// Default implementations of get ops for DataArray and LogicalArray.
macro_rules! impl_array_get {
    ($ArrayT:ty, $output:ty) => {
        impl $ArrayT {
            #[inline]
            pub fn get(&self, idx: usize) -> Option<$output> {
                if idx >= self.len() {
                    panic!("Out of bounds: {} vs len: {}", idx, self.len())
                }
                let arrow_array = self.as_arrow();
                let is_valid = arrow_array
                    .validity()
                    .map_or(true, |validity| validity.get_bit(idx));
                if is_valid {
                    Some(unsafe { arrow_array.value_unchecked(idx) })
                } else {
                    None
                }
            }
        }
    };
}

impl_array_get!(Utf8Array, &str);
impl_array_get!(BooleanArray, bool);
impl_array_get!(BinaryArray, &[u8]);
impl_array_get!(ListArray, Box<dyn arrow2::array::Array>);
impl_array_get!(FixedSizeListArray, Box<dyn arrow2::array::Array>);
impl_array_get!(Decimal128Array, i128);
impl_array_get!(DateArray, i32);
impl_array_get!(DurationArray, i64);
impl_array_get!(TimestampArray, i64);
impl_array_get!(EmbeddingArray, Box<dyn arrow2::array::Array>);
impl_array_get!(FixedShapeImageArray, Box<dyn arrow2::array::Array>);

impl NullArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<()> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        None
    }
}

impl StructArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<Vec<Box<dyn arrow2::array::Array>>> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        let arrow_array = self.as_arrow();
        let is_valid = arrow_array
            .validity()
            .map_or(true, |validity| validity.get_bit(idx));
        if is_valid {
            Some(
                arrow_array
                    .values()
                    .iter()
                    .map(|v| unsafe { v.sliced_unchecked(idx, 1) })
                    .collect(),
            )
        } else {
            None
        }
    }
}

impl ExtensionArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<Box<dyn arrow2::scalar::Scalar>> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        let is_valid = self
            .data
            .validity()
            .map_or(true, |validity| validity.get_bit(idx));
        if is_valid {
            Some(arrow2::scalar::new_scalar(self.data(), idx))
        } else {
            None
        }
    }
}

#[cfg(feature = "python")]
impl crate::datatypes::PythonArray {
    #[inline]
    pub fn get(&self, idx: usize) -> pyo3::PyObject {
        use arrow2::array::Array;
        use pyo3::prelude::*;

        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        let valid = self
            .as_arrow()
            .validity()
            .map(|vd| vd.get_bit(idx))
            .unwrap_or(true);
        if valid {
            self.as_arrow().values().get(idx).unwrap().clone()
        } else {
            Python::with_gil(|py| py.None())
        }
    }
}

impl ImageArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<Box<dyn arrow2::array::Array>> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        let arrow_array = self.as_arrow();
        let is_valid = arrow_array
            .validity()
            .map_or(true, |validity| validity.get_bit(idx));
        if is_valid {
            let data_array = arrow_array.values()[0]
                .as_any()
                .downcast_ref::<arrow2::array::ListArray<i64>>()?;
            Some(unsafe { data_array.value_unchecked(idx) })
        } else {
            None
        }
    }
}
