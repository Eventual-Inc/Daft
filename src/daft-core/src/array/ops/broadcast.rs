use std::iter::repeat_n;
use std::sync::Arc;

use common_error::{DaftError, DaftResult};
use daft_arrow::array::{MutablePrimitiveArray, PrimitiveArray};

use super::{as_arrow::AsArrow, full::FullNull};
#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    array::{
        DataArray, FixedSizeListArray, ListArray, StructArray,
    },
    datatypes::{DaftPrimitiveType, Field}, prelude::{NullArray, BooleanArray},
};

pub trait Broadcastable {
    fn broadcast(&self, num: usize) -> DaftResult<Self>
    where
        Self: Sized;
}

impl Broadcastable for NullArray {
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        Ok(Self::full_null(self.name(), self.data_type(), num))
    }
}

impl Broadcastable for BooleanArray {
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        todo!()
    }
}

impl<T> Broadcastable for DataArray<T>
where
    T: DaftPrimitiveType,
{
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }

        if self.is_valid(0) {
            let value = unsafe { self.as_arrow().value_unchecked(0) };
            let mut mutable = MutablePrimitiveArray::<T::Native>::from(
                self.data_type().to_arrow().unwrap(),
            );
            mutable.extend_trusted_len_values(repeat_n(value, num));
            let primitive_array: PrimitiveArray<T::Native> = mutable.into();
            Self::new(self.field.clone(), primitive_array.boxed())
        } else {
            Ok(Self::full_null(self.name(), self.data_type(), num))
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

        if self.is_valid(0) {
            // Get the single element and repeat it
            let single_element = self.slice(0, 1)?;
            let repeated: Vec<&Self> = repeat_n(&single_element, num).collect();
            Self::concat(&repeated)
        } else {
            Ok(Self::full_null(self.name(), self.data_type(), num))
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

        if self.is_valid(0) {
            // Get the single element and repeat it
            let single_element = self.slice(0, 1)?;
            let repeated: Vec<&Self> = repeat_n(&single_element, num).collect();
            Self::concat(&repeated)
        } else {
            Ok(Self::full_null(self.name(), self.data_type(), num))
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

        if self.is_valid(0) {
            // Broadcast each child field and reconstruct the struct
            let mut broadcasted_children = Vec::new();
            for child in self.children.iter() {
                broadcasted_children.push(child.broadcast(num)?);
            }
            Ok(Self::new(
                Arc::new(Field::new(self.name(), self.data_type().clone())),
                broadcasted_children,
                None,
            ))
        } else {
            Ok(Self::full_null(self.name(), self.data_type(), num))
        }
    }
}

#[cfg(feature = "python")]
impl Broadcastable for PythonArray {
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }

        if self.is_valid(0) {
            // Get the single element and repeat it
            let single_element = self.slice(0, 1)?;
            let repeated: Vec<&Self> = repeat_n(&single_element, num).collect();
            Self::concat(&repeated)
        } else {
            Ok(Self::full_null(self.name(), self.data_type(), num))
        }
    }
}
