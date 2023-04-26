use std::sync::Arc;

use crate::{
    array::DataArray,
    datatypes::{DaftArrowBackedType, ListArray},
    error::DaftResult,
};

use super::DaftListAggable;

use dyn_clone::clone_box;

impl<T> DaftListAggable for DataArray<T>
where
    T: DaftArrowBackedType,
{
    type Output = DaftResult<ListArray>;
    fn list(&self) -> Self::Output {
        let child_array = clone_box(self.data.as_ref() as &dyn arrow2::array::Array);
        let arrow_type = child_array.data_type();
        let offsets = arrow2::offset::OffsetsBuffer::try_from(vec![0, child_array.len() as i64])?;
        let nested_array = Box::new(arrow2::array::ListArray::<i64>::try_new(
            arrow_type.clone(),
            offsets,
            child_array,
            None,
        )?);
        let list_field = self.field.to_list_field();
        ListArray::new(Arc::new(list_field), nested_array)
    }
}
