use crate::{
    array::{BaseArray, DataArray},
    datatypes::{
        BinaryArray, BooleanArray, DaftNumericType, DataType, FixedSizeListArray, NullArray,
        Utf8Array,
    },
    error::DaftResult,
};

use super::downcast::Downcastable;

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

impl FixedSizeListArray {
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
                let repeated_values = arrow2::compute::concatenate::concatenate(
                    std::iter::repeat(val.as_ref())
                        .take(num)
                        .collect::<Vec<&dyn arrow2::array::Array>>()
                        .as_slice(),
                )?;
                if let DataType::FixedSizeList(field, _) = self.data_type() {
                    FixedSizeListArray::new(
                        self.field.clone(),
                        Box::new(arrow2::array::FixedSizeListArray::new(
                            arrow2::datatypes::DataType::FixedSizeList(
                                Box::new(field.to_arrow()?),
                                val.len(),
                            ),
                            repeated_values,
                            None,
                        )),
                    )
                } else {
                    unreachable!("should never be reached");
                }
            }
            None => Ok(DataArray::full_null(self.name(), num)),
        }
    }
}

#[cfg(feature = "python")]
impl crate::datatypes::PythonArray {
    pub fn broadcast(&self, _num: usize) -> DaftResult<Self> {
        todo!("[RUST-INT][PY] Need to implement Lit for Python objects to test this")
        // if self.len() != 1 {
        //     return Err(crate::error::DaftError::ValueError(format!(
        //         "Attempting to broadcast non-unit length Array named: {}",
        //         self.name()
        //     )));
        // }
        // let val = self.downcast().vec().iter().next().unwrap();
        // let mut repeated_values = Vec::with_capacity(num);
        // repeated_values.fill(val.clone());
        // let repeated_values_array: Box<dyn arrow2::array::Array> = Box::new(
        //     crate::array::vec_backed::VecBackedArray::new(repeated_values),
        // );
        // crate::datatypes::PythonArray::new(self.field.clone(), repeated_values_array)
    }
}
