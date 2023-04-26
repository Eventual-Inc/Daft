use std::sync::Arc;

use crate::{
    array::DataArray,
    datatypes::{DaftArrowBackedType, ListArray},
    error::DaftResult,
};

use super::{DaftListAggable, GroupIndices};

use dyn_clone::clone_box;

impl<T> DaftListAggable for DataArray<T>
where
    T: DaftArrowBackedType,
{
    type Output = DaftResult<ListArray>;
    fn list(&self) -> Self::Output {
        let child_array = clone_box(self.data.as_ref() as &dyn arrow2::array::Array);
        let offsets = arrow2::offset::OffsetsBuffer::try_from(vec![0, child_array.len() as i64])?;
        let list_field = self.field.to_list_field()?;
        let nested_array = Box::new(arrow2::array::ListArray::<i64>::try_new(
            list_field.dtype.to_arrow()?,
            offsets,
            child_array,
            None,
        )?);
        ListArray::new(Arc::new(list_field), nested_array)
    }

    fn grouped_list(&self, groups: &GroupIndices) -> Self::Output {
        let child_array = self.data.as_ref();
        let mut offsets = Vec::with_capacity(groups.len() + 1);

        offsets.push(0);

        for g in groups {
            offsets.push(offsets.last().unwrap() + g.len() as i64);
        }

        let total_capacity = *offsets.last().unwrap();
        let offsets = arrow2::offset::OffsetsBuffer::try_from(offsets)?;
        let mut growable =
            arrow2::array::growable::make_growable(&[child_array], true, total_capacity as usize);
        for g in groups {
            for idx in g {
                growable.extend(0, *idx as usize, 1);
            }
        }
        let list_field = self.field.to_list_field()?;

        let nested_array = Box::new(arrow2::array::ListArray::<i64>::try_new(
            list_field.dtype.to_arrow()?,
            offsets,
            growable.as_box(),
            None,
        )?);

        ListArray::new(Arc::new(list_field), nested_array)
    }
}
