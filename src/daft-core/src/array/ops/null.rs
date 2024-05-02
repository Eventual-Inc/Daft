use std::{iter::repeat, sync::Arc};

use crate::{
    array::{ListArray, StructArray},
    datatypes::*,
};
use common_error::DaftResult;

use super::{DaftIsNull, DaftNotNull};

impl<T> DataArray<T>
where
    T: DaftPhysicalType,
{
    // Common functionality for nullity checks
    fn check_nullity(&self, is_null: bool) -> DaftResult<DataArray<BooleanType>> {
        let arrow_array = &self.data;
        let result_arrow_array = Box::new(match arrow_array.validity() {
            // If the bitmap is None, the arrow array doesn't have null values
            // (unless it's a NullArray - so check the null count)
            None => match arrow_array.null_count() {
                0 => arrow2::array::BooleanArray::from_slice(vec![!is_null; arrow_array.len()]), // false for is_null and true for not_null
                _ => arrow2::array::BooleanArray::from_slice(vec![is_null; arrow_array.len()]), // true for is_null and false for not_null
            },
            Some(bitmap) => arrow2::array::BooleanArray::new(
                arrow2::datatypes::DataType::Boolean,
                if is_null { !bitmap } else { bitmap.clone() }, // flip the bitmap for is_null
                None,
            ),
        });
        DataArray::<BooleanType>::new(
            Arc::new(Field::new(self.field.name.clone(), DataType::Boolean)),
            result_arrow_array,
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

macro_rules! check_nullity_nested_array {
    ($arr:expr, $is_null:expr) => {{
        match $arr.validity() {
            None => Ok(BooleanArray::from((
                $arr.name(),
                repeat(!$is_null)
                    .take($arr.len())
                    .collect::<Vec<_>>()
                    .as_slice(),
            ))),
            Some(validity) => Ok(BooleanArray::from((
                $arr.name(),
                arrow2::array::BooleanArray::new(
                    arrow2::datatypes::DataType::Boolean,
                    if $is_null {
                        !validity
                    } else {
                        validity.clone()
                    },
                    None,
                ),
            ))),
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
        self.data.is_valid(idx)
    }
}

impl FixedSizeListArray {
    #[inline]
    pub fn is_valid(&self, idx: usize) -> bool {
        match self.validity() {
            None => true,
            Some(validity) => validity.get(idx).unwrap(),
        }
    }
}

impl ListArray {
    #[inline]
    pub fn is_valid(&self, idx: usize) -> bool {
        match self.validity() {
            None => true,
            Some(validity) => validity.get(idx).unwrap(),
        }
    }
}

impl StructArray {
    #[inline]
    pub fn is_valid(&self, idx: usize) -> bool {
        match self.validity() {
            None => true,
            Some(validity) => validity.get(idx).unwrap(),
        }
    }
}
