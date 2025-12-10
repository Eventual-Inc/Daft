use common_error::DaftResult;

use super::{DaftListAggable, GroupIndices};
#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    array::{
        DataArray, FixedSizeListArray, ListArray, StructArray,
        growable::{Growable, GrowableArray},
    },
    datatypes::DaftArrowBackedType,
    series::IntoSeries,
};

macro_rules! impl_daft_list_agg {
    () => {
        type Output = DaftResult<ListArray>;

        fn list(&self) -> Self::Output {
            let child_series = self.clone().into_series();
            let offsets =
                daft_arrow::offset::OffsetsBuffer::try_from(vec![0, child_series.len() as i64])?;
            let list_field = self.field().to_list_field();
            Ok(ListArray::new(list_field, child_series, offsets, None))
        }

        fn grouped_list(&self, groups: &GroupIndices) -> Self::Output {
            let mut offsets = Vec::with_capacity(groups.len() + 1);

            offsets.push(0);
            for g in groups {
                offsets.push(offsets.last().unwrap() + g.len() as i64);
            }

            let total_capacity = *offsets.last().unwrap();

            let mut growable: Box<dyn Growable> = Box::new(Self::make_growable(
                self.name(),
                self.data_type(),
                vec![self],
                self.null_count() > 0,
                total_capacity as usize,
            ));

            for g in groups {
                for idx in g {
                    growable.extend(0, *idx as usize, 1);
                }
            }
            let list_field = self.field().to_list_field();

            Ok(ListArray::new(
                list_field,
                growable.build()?,
                daft_arrow::offset::OffsetsBuffer::try_from(offsets)?,
                None,
            ))
        }
    };
}

impl<T> DaftListAggable for DataArray<T>
where
    T: DaftArrowBackedType,
    Self: IntoSeries,
    Self: GrowableArray,
{
    impl_daft_list_agg!();
}

impl DaftListAggable for ListArray {
    impl_daft_list_agg!();
}

impl DaftListAggable for FixedSizeListArray {
    impl_daft_list_agg!();
}

impl DaftListAggable for StructArray {
    impl_daft_list_agg!();
}

#[cfg(feature = "python")]
impl DaftListAggable for PythonArray {
    impl_daft_list_agg!();
}
