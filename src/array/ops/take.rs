use crate::{
    array::{BaseArray, DataArray},
    datatypes::{
        BinaryArray, BooleanArray, DaftIntegerType, DaftNumericType, FixedSizeListArray, ListArray,
        NullArray, Utf8Array,
    },
    error::DaftResult,
};

use super::downcast::Downcastable;

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    #[inline]
    pub fn get(&self, idx: usize) -> Option<T::Native> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        let arrow_array = self.downcast();
        let is_valid = match arrow_array.validity() {
            Some(validity) => validity.get_bit(idx),
            None => true,
        };
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
        let result = arrow2::compute::take::take(self.data(), idx.downcast())?;
        Self::try_from((self.name(), result))
    }

    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(format!("{v}")),
        }
    }
}

impl Utf8Array {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<&str> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        let arrow_array = self.downcast();
        let is_valid = match arrow_array.validity() {
            Some(validity) => validity.get_bit(idx),
            None => true,
        };
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
        let result = arrow2::compute::take::take(self.data(), idx.downcast())?;
        Self::try_from((self.name(), result))
    }

    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(format!("\"{v}\"")),
        }
    }
}

impl BooleanArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        let arrow_array = self.downcast();
        let is_valid = match arrow_array.validity() {
            Some(validity) => validity.get_bit(idx),
            None => true,
        };
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
        let result = arrow2::compute::take::take(self.data(), idx.downcast())?;
        Self::try_from((self.name(), result))
    }

    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(format!("{v}")),
        }
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
        let result = arrow2::compute::take::take(self.data(), idx.downcast())?;
        Self::try_from((self.name(), result))
    }

    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        Ok("None".to_string())
    }
}

impl BinaryArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<&[u8]> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        let arrow_array = self.downcast();
        let is_valid = match arrow_array.validity() {
            Some(validity) => validity.get_bit(idx),
            None => true,
        };
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
        let result = arrow2::compute::take::take(self.data(), idx.downcast())?;
        Self::try_from((self.name(), result))
    }

    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            // TODO: [RUST-INT] proper display of bytes as string here, preferably similar to how Python displays it
            // See discussion: https://stackoverflow.com/questions/54358833/how-does-bytes-repr-representation-work
            Some(v) => Ok(format!("b\"{:?}\"", v)),
        }
    }
}

impl ListArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<Box<dyn arrow2::array::Array>> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        let arrow_array = self.downcast();
        let is_valid = match arrow_array.validity() {
            Some(validity) => validity.get_bit(idx),
            None => true,
        };
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
        let result = arrow2::compute::take::take(self.data(), idx.downcast())?;
        Self::try_from((self.name(), result))
    }

    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            // TODO: [RUST-INT] proper display of bytes as string here, preferably similar to how Python displays it
            // See discussion: https://stackoverflow.com/questions/54358833/how-does-bytes-repr-representation-work
            Some(v) => Ok(format!("b\"{:?}\"", v)),
        }
    }
}

impl FixedSizeListArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<Box<dyn arrow2::array::Array>> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        let arrow_array = self.downcast();
        let is_valid = match arrow_array.validity() {
            Some(validity) => validity.get_bit(idx),
            None => true,
        };
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
        let result = arrow2::compute::take::take(self.data(), idx.downcast())?;
        Self::try_from((self.name(), result))
    }

    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            // TODO: [RUST-INT] proper display of bytes as string here, preferably similar to how Python displays it
            // See discussion: https://stackoverflow.com/questions/54358833/how-does-bytes-repr-representation-work
            Some(v) => Ok(format!("b\"{:?}\"", v)),
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
            .downcast()
            .validity()
            .map(|vd| vd.get_bit(idx))
            .unwrap_or(true);
        if valid {
            self.downcast().values().get(idx).unwrap().clone()
        } else {
            Python::with_gil(|py| py.None())
        }
    }

    pub fn take<I>(&self, _idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        todo!()
        // use arrow2::types::Index;
        // use pyo3::prelude::*;

        // use crate::array::pseudo_arrow::PseudoArrowArray;

        // let indices = idx.downcast();

        // let old_values = self.downcast().values();

        // let new_values: Vec<PyObject> = {
        //     let py_none = Python::with_gil(|py| {
        //         py.None()
        //     });

        //     indices
        //         .iter()
        //         .map(|maybe_idx| match maybe_idx {
        //             Some(idx) => old_values[idx.to_usize()].clone(),
        //             None => py_none.clone(),
        //         })
        //         .collect()
        // };

        // let new_validity = {
        //     self.downcast().validity().map(|old_validity| {
        //         let old_validity_array = {
        //             &arrow2::array::BooleanArray::new(
        //                 arrow2::datatypes::DataType::Boolean,
        //                 old_validity.clone(),
        //                 None,
        //             )
        //         };
        //         let new_validity_array = {
        //             arrow2::compute::take::take(old_validity_array, indices)?
        //                 .as_any()
        //                 .downcast_ref::<arrow2::array::BooleanArray>()
        //                 .unwrap()
        //         };
        //         arrow2::bitmap::Bitmap::from_iter(
        //             new_validity_array
        //                 .iter()
        //                 .map(|valid| valid.unwrap_or(false)),
        //         )
        //     })
        // };

        // let arrow_array: Box<dyn arrow2::array::Array> =
        //     Box::new(PseudoArrowArray::<PyObject>::from_pyobj_vec(values_vec));

        // DataArray::<crate::datatypes::PythonType>::new(self.field().clone().into(), arrow_array)
    }

    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        use pyo3::prelude::*;

        let val = self.get(idx);

        let call_result =
            Python::with_gil(|py| val.call_method0(py, pyo3::intern!(py, "__str__")))?;

        let extracted = Python::with_gil(|py| call_result.extract(py))?;

        Ok(extracted)
    }
}
