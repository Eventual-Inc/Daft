use crate::{
    array::DataArray,
    datatypes::{
        logical::{
            DateArray, Decimal128Array, DurationArray, ImageArray, LogicalArrayImpl, TimestampArray,
        },
        nested_arrays::FixedSizeListArray,
        BinaryArray, BooleanArray, DaftLogicalType, DaftNumericType, ExtensionArray, ListArray,
        NullArray, StructArray, Utf8Array,
    },
    Series,
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
macro_rules! impl_array_arrow_get {
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

impl<L: DaftLogicalType> LogicalArrayImpl<L, FixedSizeListArray> {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<Series> {
        self.physical.get(idx)
    }
}

impl_array_arrow_get!(Utf8Array, &str);
impl_array_arrow_get!(BooleanArray, bool);
impl_array_arrow_get!(BinaryArray, &[u8]);
impl_array_arrow_get!(ListArray, Box<dyn arrow2::array::Array>);
impl_array_arrow_get!(Decimal128Array, i128);
impl_array_arrow_get!(DateArray, i32);
impl_array_arrow_get!(DurationArray, i64);
impl_array_arrow_get!(TimestampArray, i64);

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

impl FixedSizeListArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<Series> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
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

#[cfg(test)]
mod tests {
    use common_error::DaftResult;

    use crate::{
        datatypes::{nested_arrays::FixedSizeListArray, BooleanArray, Field, Int32Array},
        DataType, IntoSeries,
    };

    #[test]
    fn test_fixed_size_list_get_all_valid() -> DaftResult<()> {
        let field = Field::new(
            "foo",
            DataType::FixedSizeList(Box::new(Field::new("foo", DataType::Int32)), 3),
        );
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
                .map(|x| x.map(|v| *v))
                .collect::<Vec<Option<i32>>>();
            let expected = ((i * 3) as i32..((i + 1) * 3) as i32)
                .map(|x| Some(x))
                .collect::<Vec<Option<i32>>>();
            assert_eq!(data, expected);
        }

        Ok(())
    }

    #[test]
    fn test_fixed_size_list_get_some_valid() -> DaftResult<()> {
        let field = Field::new(
            "foo",
            DataType::FixedSizeList(Box::new(Field::new("foo", DataType::Int32)), 3),
        );
        let flat_child = Int32Array::from(("foo", (0..9).collect::<Vec<i32>>()));
        let raw_validity = vec![true, false, true];
        let validity = Some(arrow2::bitmap::Bitmap::from(raw_validity.as_slice()));
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
            .map(|x| x.map(|v| *v))
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
            .map(|x| x.map(|v| *v))
            .collect::<Vec<Option<i32>>>();
        let expected = vec![Some(3), Some(4), Some(5)];
        assert_eq!(data, expected);

        Ok(())
    }
}
