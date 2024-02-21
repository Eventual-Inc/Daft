use common_error::DaftResult;

use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::DaftPhysicalType,
};

use super::DaftAnyValueAggable;

impl<T> DaftAnyValueAggable for DataArray<T>
where
    T: DaftPhysicalType,
{
    type Output = DaftResult<DataArray<T>>;

    fn any_value(&self) -> Self::Output {
        todo!()
    }

    fn grouped_any_value(&self, _groups: &super::GroupIndices) -> Self::Output {
        todo!()
    }
}

macro_rules! impl_daft_any_value_nested_array {
    ($arr:ident) => {
        impl DaftAnyValueAggable for $arr {
            type Output = DaftResult<$arr>;

            fn any_value(&self) -> Self::Output {
                todo!()
            }

            fn grouped_any_value(&self, _groups: &super::GroupIndices) -> Self::Output {
                todo!()
            }
        }
    };
}

impl_daft_any_value_nested_array!(FixedSizeListArray);
impl_daft_any_value_nested_array!(ListArray);
impl_daft_any_value_nested_array!(StructArray);
