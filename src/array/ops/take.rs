use crate::{
    array::DataArray,
    datatypes::{
        logical::{DateArray, EmbeddingArray, FixedShapeImageArray, ImageArray, TimestampArray},
        BinaryArray, BooleanArray, DaftIntegerType, DaftNumericType, ExtensionArray,
        FixedSizeListArray, ListArray, NullArray, StructArray, Utf8Array,
    },
    error::DaftResult,
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

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let result = arrow2::compute::take::take(self.data(), idx.as_arrow())?;
        Self::try_from((self.field.clone(), result))
    }
}

impl Utf8Array {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<&str> {
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

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let result = arrow2::compute::take::take(self.data(), idx.as_arrow())?;
        Self::try_from((self.field.clone(), result))
    }
}

impl BooleanArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<bool> {
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

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let result = arrow2::compute::take::take(self.data(), idx.as_arrow())?;
        Self::try_from((self.field.clone(), result))
    }
}

impl NullArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<()> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        None
    }

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let result = arrow2::compute::take::take(self.data(), idx.as_arrow())?;
        Self::try_from((self.field.clone(), result))
    }
}

impl BinaryArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<&[u8]> {
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

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let result = arrow2::compute::take::take(self.data(), idx.as_arrow())?;
        Self::try_from((self.field.clone(), result))
    }
}

impl ListArray {
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
            Some(unsafe { arrow_array.value_unchecked(idx) })
        } else {
            None
        }
    }

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let result = arrow2::compute::take::take(self.data(), idx.as_arrow())?;
        Self::try_from((self.field.clone(), result))
    }
}

impl FixedSizeListArray {
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
            Some(unsafe { arrow_array.value_unchecked(idx) })
        } else {
            None
        }
    }

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let result = arrow2::compute::take::take(self.data(), idx.as_arrow())?;
        Self::try_from((self.field.clone(), result))
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

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let result = arrow2::compute::take::take(self.data(), idx.as_arrow())?;
        Self::try_from((self.field.clone(), result))
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

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let result = arrow2::compute::take::take(self.data(), idx.as_arrow())?;
        Self::try_from((self.field.clone(), result))
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

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        use crate::array::pseudo_arrow::PseudoArrowArray;
        use crate::datatypes::PythonType;

        use arrow2::array::Array;
        use arrow2::types::Index;
        use pyo3::prelude::*;

        let indices = idx.as_arrow();

        let old_values = self.as_arrow().values();

        // Execute take on the data values, ignoring validity.
        let new_values: Vec<PyObject> = {
            let py_none = Python::with_gil(|py: Python| py.None());

            indices
                .iter()
                .map(|maybe_idx| match maybe_idx {
                    Some(idx) => old_values[idx.to_usize()].clone(),
                    None => py_none.clone(),
                })
                .collect()
        };

        // Execute take on the validity bitmap using arrow2::compute.
        let new_validity = {
            self.as_arrow()
                .validity()
                .map(|old_validity| {
                    let old_validity_array = {
                        &arrow2::array::BooleanArray::new(
                            arrow2::datatypes::DataType::Boolean,
                            old_validity.clone(),
                            None,
                        )
                    };
                    arrow2::compute::take::take(old_validity_array, indices)
                })
                .transpose()?
                .map(|new_validity_dynarray| {
                    let new_validity_iter = new_validity_dynarray
                        .as_any()
                        .downcast_ref::<arrow2::array::BooleanArray>()
                        .unwrap()
                        .iter();
                    arrow2::bitmap::Bitmap::from_iter(
                        new_validity_iter.map(|valid| valid.unwrap_or(false)),
                    )
                })
        };

        let arrow_array: Box<dyn arrow2::array::Array> =
            Box::new(PseudoArrowArray::new(new_values.into(), new_validity));

        DataArray::<PythonType>::new(self.field().clone().into(), arrow_array)
    }
}

impl DateArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<i32> {
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

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let new_array = self.physical.take(idx)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}

impl TimestampArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<i64> {
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

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let new_array = self.physical.take(idx)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}

impl EmbeddingArray {
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
            Some(unsafe { arrow_array.value_unchecked(idx) })
        } else {
            None
        }
    }

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let new_array = self.physical.take(idx)?;
        Ok(Self::new(self.field.clone(), new_array))
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

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let new_array = self.physical.take(idx)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}

impl FixedShapeImageArray {
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
            Some(unsafe { arrow_array.value_unchecked(idx) })
        } else {
            None
        }
    }

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let new_array = self.physical.take(idx)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}
