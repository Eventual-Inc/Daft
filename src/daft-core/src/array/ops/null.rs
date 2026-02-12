use std::{iter::repeat, ops::Not, sync::Arc};

use common_error::DaftResult;

use super::{DaftIsNull, DaftNotNull};
#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    array::{ListArray, StructArray},
    datatypes::*,
};

impl<T> DataArray<T>
where
    T: DaftPhysicalType,
{
    // Common functionality for nullity checks
    fn check_nullity(&self, is_null: bool) -> DaftResult<DataArray<BooleanType>> {
        let values = match self.nulls() {
            // If there's no null buffer, check null_count for the NullArray edge case
            None => {
                let all_valid = self.null_count() == 0;
                if all_valid != is_null {
                    arrow::buffer::BooleanBuffer::new_set(self.len())
                } else {
                    arrow::buffer::BooleanBuffer::new_unset(self.len())
                }
            }
            Some(nulls) => {
                if is_null {
                    nulls.inner().not()
                } else {
                    nulls.clone().into_inner()
                }
            }
        };
        BooleanArray::from_arrow(
            Field::new(self.field.name.clone(), DataType::Boolean),
            Arc::new(arrow::array::BooleanArray::new(values, None)),
        )
    }
}

impl<T> DaftIsNull for DataArray<T>
where
    T: DaftPhysicalType,
{
    type Output = DaftResult<DataArray<BooleanType>>;

    fn is_null(&self) -> Self::Output {
        self.check_nullity(true)
    }
}

impl<T> DaftNotNull for DataArray<T>
where
    T: DaftPhysicalType,
{
    type Output = DaftResult<DataArray<BooleanType>>;

    fn not_null(&self) -> Self::Output {
        self.check_nullity(false)
    }
}

#[cfg(feature = "python")]
impl PythonArray {
    // Common functionality for nullity checks
    fn check_nullity(&self, is_null: bool) -> DaftResult<DataArray<BooleanType>> {
        let values = if let Some(nulls) = self.nulls() {
            if is_null {
                nulls.inner().not()
            } else {
                nulls.clone().into_inner()
            }
        } else if is_null {
            daft_arrow::buffer::NullBuffer::new_null(self.len()).into_inner()
        } else {
            daft_arrow::buffer::NullBuffer::new_valid(self.len()).into_inner()
        };

        BooleanArray::from_arrow(
            Field::new(self.name(), DataType::Boolean),
            Arc::new(arrow::array::BooleanArray::new(values, None)),
        )
    }
}

#[cfg(feature = "python")]
impl DaftIsNull for PythonArray {
    type Output = DaftResult<DataArray<BooleanType>>;

    fn is_null(&self) -> Self::Output {
        self.check_nullity(true)
    }
}

#[cfg(feature = "python")]
impl DaftNotNull for PythonArray {
    type Output = DaftResult<DataArray<BooleanType>>;

    fn not_null(&self) -> Self::Output {
        self.check_nullity(false)
    }
}

macro_rules! check_nullity_nested_array {
    ($arr:expr, $is_null:expr) => {{
        match $arr.nulls() {
            None => Ok(BooleanArray::from_values(
                $arr.name(),
                repeat(!$is_null).take($arr.len()),
            )),
            Some(nulls) => BooleanArray::from_arrow(
                Field::new($arr.name(), DataType::Boolean),
                Arc::new(arrow::array::BooleanArray::new(
                    if $is_null {
                        nulls.inner().not()
                    } else {
                        nulls.clone().into_inner()
                    },
                    None,
                )),
            ),
        }
    }};
}

macro_rules! impl_is_null_nested_array {
    ($arr:ident) => {
        impl DaftIsNull for $arr {
            type Output = DaftResult<DataArray<BooleanType>>;

            fn is_null(&self) -> Self::Output {
                check_nullity_nested_array!(self, true)
            }
        }
    };
}

macro_rules! impl_not_null_nested_array {
    ($arr:ident) => {
        impl DaftNotNull for $arr {
            type Output = DaftResult<DataArray<BooleanType>>;

            fn not_null(&self) -> Self::Output {
                check_nullity_nested_array!(self, false)
            }
        }
    };
}

impl_is_null_nested_array!(ListArray);
impl_is_null_nested_array!(FixedSizeListArray);
impl_is_null_nested_array!(StructArray);

impl_not_null_nested_array!(ListArray);
impl_not_null_nested_array!(FixedSizeListArray);
impl_not_null_nested_array!(StructArray);

impl<T> DataArray<T>
where
    T: DaftPhysicalType,
{
    #[inline]
    pub fn is_valid(&self, idx: usize) -> bool {
        match self.nulls() {
            None => self.null_count() == 0,
            Some(nulls) => nulls.is_valid(idx),
        }
    }
}

impl FixedSizeListArray {
    #[inline]
    pub fn is_valid(&self, idx: usize) -> bool {
        match self.nulls() {
            None => true,
            Some(nulls) => nulls.is_valid(idx),
        }
    }
}

impl ListArray {
    #[inline]
    pub fn is_valid(&self, idx: usize) -> bool {
        match self.nulls() {
            None => true,
            Some(nulls) => nulls.is_valid(idx),
        }
    }
}

impl StructArray {
    #[inline]
    pub fn is_valid(&self, idx: usize) -> bool {
        match self.nulls() {
            None => true,
            Some(nulls) => nulls.is_valid(idx),
        }
    }
}
