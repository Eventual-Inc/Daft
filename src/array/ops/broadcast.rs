use crate::{
    array::{BaseArray, DataArray},
    datatypes::{DaftNumericType, Utf8Array},
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
        let val = self.downcast().iter().next().unwrap();
        if let Some(v) = val {
            let repeated_values: Vec<<T as DaftNumericType>::Native> =
                std::iter::repeat(*v).take(num).collect();
            return Ok(DataArray::from((self.name(), repeated_values.as_slice())));
        } else {
            return Ok(DataArray::full_null(self.name(), num));
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
        let val = self.downcast().iter().next().unwrap();
        if let Some(s) = val {
            let repeated_values: Vec<&str> = std::iter::repeat(s).take(num).collect();
            return Ok(DataArray::from((self.name(), repeated_values.as_slice())));
        } else {
            return Ok(DataArray::full_null(self.name(), num));
        }
    }
}
