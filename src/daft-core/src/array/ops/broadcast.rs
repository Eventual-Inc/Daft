use crate::{
    array::DataArray,
    datatypes::{
        BinaryArray, BooleanArray, DaftNumericType, DataType, ExtensionArray, FixedSizeListArray,
        ListArray, NullArray, StructArray, Utf8Array,
    },
};

use common_error::{DaftError, DaftResult};

use super::as_arrow::AsArrow;

#[cfg(feature = "python")]
use crate::array::pseudo_arrow::PseudoArrowArray;
#[cfg(feature = "python")]
use crate::datatypes::PythonArray;

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
        let maybe_val = self.as_arrow().iter().next().unwrap();
        match maybe_val {
            Some(val) => {
                let repeated_values: Vec<<T as DaftNumericType>::Native> =
                    std::iter::repeat(*val).take(num).collect();
                return Ok(DataArray::from((self.name(), repeated_values.as_slice())));
            }
            None => Ok(DataArray::full_null(self.name(), self.data_type(), num)),
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
        let maybe_val = self.as_arrow().iter().next().unwrap();
        match maybe_val {
            Some(val) => {
                let repeated_values: Vec<&str> = std::iter::repeat(val).take(num).collect();
                Ok(DataArray::from((self.name(), repeated_values.as_slice())))
            }
            None => Ok(DataArray::full_null(self.name(), self.data_type(), num)),
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
        Ok(DataArray::full_null(self.name(), self.data_type(), num))
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
        let maybe_val = self.as_arrow().iter().next().unwrap();
        match maybe_val {
            Some(val) => {
                let repeated_values: Vec<bool> = std::iter::repeat(val).take(num).collect();
                Ok(DataArray::from((self.name(), repeated_values.as_slice())))
            }
            None => Ok(DataArray::full_null(self.name(), self.data_type(), num)),
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
        let maybe_val = self.as_arrow().iter().next().unwrap();
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
            None => Ok(DataArray::full_null(self.name(), self.data_type(), num)),
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
        let maybe_val = self.as_arrow().iter().next().unwrap();
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
            None => Ok(DataArray::full_null(self.name(), self.data_type(), num)),
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
        let maybe_val = self.as_arrow().iter().next().unwrap();
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
            None => Ok(DataArray::full_null(self.name(), self.data_type(), num)),
        }
    }
}

impl Broadcastable for StructArray {
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }
        let arrow_arr: &arrow2::array::StructArray = self.as_arrow();
        let arrays = arrow_arr
            .values()
            .iter()
            .map(|field_arr| {
                arrow2::compute::concatenate::concatenate(
                    std::iter::repeat(field_arr.as_ref())
                        .take(num)
                        .collect::<Vec<&dyn arrow2::array::Array>>()
                        .as_slice(),
                )
            })
            .collect::<Result<Vec<Box<dyn arrow2::array::Array>>, _>>()?;
        let validity = arrow_arr.validity().map(|v| {
            arrow2::bitmap::Bitmap::from_iter(std::iter::repeat(v.iter().next().unwrap()).take(num))
        });
        match self.data_type() {
            DataType::Struct(fields) => StructArray::new(
                self.field.clone(),
                Box::new(arrow2::array::StructArray::new(
                    arrow2::datatypes::DataType::Struct(
                        fields
                            .iter()
                            .map(|field| field.to_arrow())
                            .collect::<DaftResult<Vec<arrow2::datatypes::Field>>>()?,
                    ),
                    arrays,
                    validity,
                )),
            ),
            other => unreachable!(
                "Data type for StructArray should always be DataType::Struct, but got: {}",
                other
            ),
        }
    }
}

impl Broadcastable for ExtensionArray {
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }
        let array = self.data();
        let mut growable = arrow2::array::growable::make_growable(&[array], true, num);
        for _ in 0..num {
            growable.extend(0, 0, 1);
        }
        ExtensionArray::new(self.field.clone(), growable.as_box())
    }
}

#[cfg(feature = "python")]
impl Broadcastable for crate::datatypes::PythonArray {
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        use pyo3::prelude::*;

        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }

        let val = self.get(0);

        let repeated_values = vec![val.clone(); num];

        let validity = {
            let is_none = Python::with_gil(|py| val.is_none(py));
            match is_none {
                true => Some(arrow2::bitmap::Bitmap::new_zeroed(num)),
                false => None,
            }
        };

        let repeated_values_array: Box<dyn arrow2::array::Array> =
            Box::new(PseudoArrowArray::new(repeated_values.into(), validity));
        PythonArray::new(self.field.clone(), repeated_values_array)
    }
}
