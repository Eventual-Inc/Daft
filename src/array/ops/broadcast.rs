use crate::{
    array::{BaseArray, DataArray},
    datatypes::{
        BinaryArray, BooleanArray, DaftNumericType, DataType, FixedSizeListArray, ListArray,
        NullArray, Utf8Array,
    },
    error::{DaftError, DaftResult},
};

use super::downcast::Downcastable;

pub trait Broadcastable {
    fn broadcast(&self, num: usize) -> DaftResult<Self>
    where
        Self: Sized;
}

impl<T> Broadcastable for DataArray<T>
where
    T: DaftNumericType,
{
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
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

impl Broadcastable for Utf8Array {
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
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

impl Broadcastable for NullArray {
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }
        Ok(DataArray::full_null(self.name(), num))
    }
}

impl Broadcastable for BooleanArray {
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
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

impl Broadcastable for BinaryArray {
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
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

impl Broadcastable for ListArray {
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }
        let maybe_val = self.downcast().iter().next().unwrap();
        match maybe_val {
            Some(val) => match i64::try_from(val.len()) {
                Ok(array_length) => {
                    let repeated_values = arrow2::compute::concatenate::concatenate(
                        std::iter::repeat(val.as_ref())
                            .take(num)
                            .collect::<Vec<&dyn arrow2::array::Array>>()
                            .as_slice(),
                    )?;
                    let mut accum = Vec::with_capacity(num + 1);
                    accum.push(0);
                    let offsets = arrow2::offset::OffsetsBuffer::<i64>::try_from(
                        std::iter::repeat(array_length)
                            .take(num)
                            .fold(accum, |mut acc, x| {
                                acc.push(x + acc.last().unwrap());
                                acc
                            }),
                    )?;
                    match self.data_type() {
                        DataType::List(field) => ListArray::new(
                            self.field.clone(),
                            Box::new(
                                arrow2::array::ListArray::<i64>::new(
                                    arrow2::datatypes::DataType::LargeList(Box::new(field.to_arrow()?)),
                                    offsets,
                                    repeated_values,
                                    None,
                                )
                            ),
                        ),
                        other => unreachable!("Data type for ListArray should always be DataType::List, but got: {}", other),
                    }
                }
                Err(e) => Err(DaftError::ValueError(format!(
                    "Unable to create ListArray during broadcasting for array named {} due to being unable to cast array length to i64: {}",
                    self.name(), e,
                ))),
            }
            None => Ok(DataArray::full_null(self.name(), num)),
        }
    }
}

impl Broadcastable for FixedSizeListArray {
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
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
                match self.data_type() {
                    DataType::FixedSizeList(field, _) => FixedSizeListArray::new(
                        self.field.clone(),
                        Box::new(arrow2::array::FixedSizeListArray::new(
                            arrow2::datatypes::DataType::FixedSizeList(
                                Box::new(field.to_arrow()?),
                                val.len(),
                            ),
                            repeated_values,
                            None,
                        )),
                    ),
                    other => unreachable!(
                        "Data type for ListArray should always be DataType::List, but got: {}",
                        other
                    ),
                }
            }
            None => Ok(DataArray::full_null(self.name(), num)),
        }
    }
}

#[cfg(feature = "python")]
impl Broadcastable for crate::datatypes::PythonArray {
    fn broadcast(&self, _num: usize) -> DaftResult<Self> {
        todo!("[RUST-INT][PY] Need to implement Lit for Python objects to test this")
        // if self.len() != 1 {
        //     return Err(DaftError::ValueError(format!(
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
