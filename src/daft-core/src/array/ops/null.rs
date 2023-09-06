use std::{iter::repeat, sync::Arc};

use arrow2;

use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::*,
};
use common_error::DaftResult;

use super::DaftIsNull;

impl<T> DaftIsNull for DataArray<T>
where
    T: DaftPhysicalType,
{
    type Output = DaftResult<DataArray<BooleanType>>;

    fn is_null(&self) -> Self::Output {
        let arrow_array = &self.data;
        let result_arrow_array = Box::new(match arrow_array.validity() {
            // If the bitmap is None, the arrow array doesn't have null values
            // (unless it's a NullArray - so check the null count)
            None => match arrow_array.null_count() {
                0 => arrow2::array::BooleanArray::from_slice(vec![false; arrow_array.len()]),
                _ => arrow2::array::BooleanArray::from_slice(vec![true; arrow_array.len()]),
            },
            Some(bitmap) => arrow2::array::BooleanArray::new(
                arrow2::datatypes::DataType::Boolean,
                !bitmap,
                None,
            ),
        });
        DataArray::<BooleanType>::new(
            Arc::new(Field::new(self.field.name.clone(), DataType::Boolean)),
            result_arrow_array,
        )
    }
}

macro_rules! impl_is_null_nested_array {
    ($arr:ident) => {
        impl DaftIsNull for $arr {
            type Output = DaftResult<DataArray<BooleanType>>;

            fn is_null(&self) -> Self::Output {
                match self.validity() {
                    None => Ok(BooleanArray::from((
                        self.name(),
                        repeat(false)
                            .take(self.len())
                            .collect::<Vec<_>>()
                            .as_slice(),
                    ))),
                    Some(validity) => Ok(BooleanArray::from((
                        self.name(),
                        arrow2::array::BooleanArray::new(
                            arrow2::datatypes::DataType::Boolean,
                            validity.clone(),
                            None,
                        ),
                    ))),
                }
            }
        }
    };
}

impl_is_null_nested_array!(ListArray);
impl_is_null_nested_array!(FixedSizeListArray);
impl_is_null_nested_array!(StructArray);

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
