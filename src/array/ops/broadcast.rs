use pyo3::Python;

use crate::{
    array::{vec_backed::VecBackedArray, BaseArray, DataArray},
    datatypes::{BinaryArray, BooleanArray, DaftNumericType, NullArray, PythonArray, Utf8Array},
    error::DaftResult,
};

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(crate::error::DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }
        let maybe_val = self.downcast().iter().next().unwrap();
        match maybe_val {
            Some(val) => {
                let repeated_values: Vec<<T as DaftNumericType>::Native> =
                    std::iter::repeat(*val).take(num).collect();
                return Ok(DataArray::from((self.name(), repeated_values.as_slice())));
            }
            None => Ok(DataArray::full_null(self.name(), num)),
        }
    }
}

impl Utf8Array {
    pub fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(crate::error::DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }
        let maybe_val = self.downcast().iter().next().unwrap();
        match maybe_val {
            Some(val) => {
                let repeated_values: Vec<&str> = std::iter::repeat(val).take(num).collect();
                Ok(DataArray::from((self.name(), repeated_values.as_slice())))
            }
            None => Ok(DataArray::full_null(self.name(), num)),
        }
    }
}

impl NullArray {
    pub fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(crate::error::DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }
        Ok(DataArray::full_null(self.name(), num))
    }
}

impl BooleanArray {
    pub fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(crate::error::DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }
        let maybe_val = self.downcast().iter().next().unwrap();
        match maybe_val {
            Some(val) => {
                let repeated_values: Vec<bool> = std::iter::repeat(val).take(num).collect();
                Ok(DataArray::from((self.name(), repeated_values.as_slice())))
            }
            None => Ok(DataArray::full_null(self.name(), num)),
        }
    }
}

impl BinaryArray {
    pub fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(crate::error::DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }
        let maybe_val = self.downcast().iter().next().unwrap();
        match maybe_val {
            Some(val) => {
                let repeated_values: Vec<&[u8]> = std::iter::repeat(val).take(num).collect();
                BinaryArray::new(
                    self.field.clone(),
                    Box::new(arrow2::array::BinaryArray::<i64>::from_slice(
                        repeated_values.as_slice(),
                    )),
                )
            }
            None => Ok(DataArray::full_null(self.name(), num)),
        }
    }
}

impl PythonArray {
    pub fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(crate::error::DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }
        let val = self.downcast().vec().iter().next().unwrap();
        let mut repeated_values = Vec::with_capacity(num);
        Python::with_gil(|py| repeated_values.fill(val.clone_ref(py)));
        let repeated_values_array: Box<dyn arrow2::array::Array> =
            Box::new(VecBackedArray::new(repeated_values, None));
        PythonArray::new(self.field.clone(), repeated_values_array)
    }
}
