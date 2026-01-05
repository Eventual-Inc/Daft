use std::{iter::repeat_n, sync::Arc};

use arrow::array::{
    BooleanBuilder, FixedSizeBinaryBuilder, IntervalMonthDayNanoBuilder, LargeBinaryBuilder,
    LargeStringBuilder, PrimitiveBuilder,
};
use common_error::{DaftError, DaftResult};

use super::full::FullNull;
#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::{DaftPrimitiveType, Field, NumericNative},
    prelude::{
        AsArrow, BinaryType, BooleanArray, ExtensionArray, FixedSizeBinaryType, FromArrow,
        IntervalType, NullArray, Utf8Type,
    },
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
        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }

        if self.is_valid(0) {
            let mut builder = BooleanBuilder::with_capacity(num);
            builder.append_n(num, self.get(0).unwrap());
            Self::from_arrow(self.field.clone(), Arc::new(builder.finish()))
        } else {
            Ok(Self::full_null(self.name(), self.data_type(), num))
        }
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
            let value = self.as_arrow()?.value(0);
            let mut builder =
                PrimitiveBuilder::<<T::Native as NumericNative>::ARROWTYPE>::with_capacity(num);
            builder.append_value_n(value, num);

            Self::from_arrow(self.field.clone(), Arc::new(builder.finish()))
        } else {
            Ok(Self::full_null(self.name(), self.data_type(), num))
        }
    }
}

impl Broadcastable for DataArray<IntervalType> {
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }

        if self.is_valid(0) {
            let arrow_array = self.as_arrow()?;
            let value = arrow_array.value(0);
            let mut builder = IntervalMonthDayNanoBuilder::with_capacity(num);
            for _ in 0..num {
                builder.append_value(value);
            }
            Self::from_arrow(self.field.clone(), Arc::new(builder.finish()))
        } else {
            Ok(Self::full_null(self.name(), self.data_type(), num))
        }
    }
}

impl Broadcastable for DataArray<BinaryType> {
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }

        if self.is_valid(0) {
            let arrow_array = self.as_arrow()?;
            let value = arrow_array.value(0);
            let mut builder = LargeBinaryBuilder::with_capacity(num, num * value.len());
            for _ in 0..num {
                builder.append_value(value);
            }
            Self::from_arrow(self.field.clone(), Arc::new(builder.finish()))
        } else {
            Ok(Self::full_null(self.name(), self.data_type(), num))
        }
    }
}

impl Broadcastable for DataArray<FixedSizeBinaryType> {
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }

        if self.is_valid(0) {
            let arrow_array = self.as_arrow()?;
            let value = arrow_array.value(0);
            let mut builder = FixedSizeBinaryBuilder::with_capacity(num, value.len() as i32);
            for _ in 0..num {
                builder.append_value(value)?;
            }
            Self::from_arrow(self.field.clone(), Arc::new(builder.finish()))
        } else {
            Ok(Self::full_null(self.name(), self.data_type(), num))
        }
    }
}

impl Broadcastable for DataArray<Utf8Type> {
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }

        if self.is_valid(0) {
            let arrow_array = self.as_arrow()?;
            let value = arrow_array.value(0);
            let mut builder = LargeStringBuilder::with_capacity(num, num * value.len());
            for _ in 0..num {
                builder.append_value(value);
            }
            Self::from_arrow(self.field.clone(), Arc::new(builder.finish()))
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
            for child in &self.children {
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

impl Broadcastable for ExtensionArray {
    fn broadcast(&self, num: usize) -> DaftResult<Self> {
        if self.len() != 1 {
            return Err(DaftError::ValueError(format!(
                "Attempting to broadcast non-unit length Array named: {}",
                self.name()
            )));
        }

        if self.is_valid(0) {
            let single_element = self.slice(0, 1)?;
            let repeated: Vec<&Self> = repeat_n(&single_element, num).collect();
            Self::concat(&repeated)
        } else {
            Ok(Self::full_null(self.name(), self.data_type(), num))
        }
    }
}
