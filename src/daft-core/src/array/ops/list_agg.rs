use common_error::DaftResult;

use super::{DaftListAggable, GroupIndices};
#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::DaftArrowBackedType,
    prelude::UInt64Array,
    series::IntoSeries,
};

macro_rules! impl_daft_list_agg {
    () => {
        type Output = DaftResult<ListArray>;

        fn list(&self) -> Self::Output {
            let child_series = self.clone().into_series();
            let offsets =
                arrow::buffer::OffsetBuffer::new(vec![0, child_series.len() as i64].into());
            let list_field = self.field().to_list_field();
            Ok(ListArray::new(list_field, child_series, offsets, None))
        }

        fn grouped_list(&self, groups: &GroupIndices) -> Self::Output {
            let mut offsets = Vec::with_capacity(groups.len() + 1);
            offsets.push(0);
            for g in groups {
                offsets.push(offsets.last().unwrap() + g.len() as i64);
            }
            eprintln!(
                "offsets memory usage: {} bytes",
                offsets.len() * std::mem::size_of::<i64>()
            );

            let idxs = groups
                .iter()
                .flat_map(|g| g.iter().copied())
                .collect::<Vec<_>>();
            eprintln!(
                "idxs memory usage: {} bytes",
                idxs.len() * std::mem::size_of::<u64>()
            );
            let child_series = self.take(&UInt64Array::from_vec("", idxs))?;
            let list_field = self.field().to_list_field();

            Ok(ListArray::new(
                list_field,
                child_series.into_series(),
                arrow::buffer::OffsetBuffer::new(offsets.into()),
                None,
            ))
        }
    };
}

impl<T> DaftListAggable for DataArray<T>
where
    T: DaftArrowBackedType,
    Self: IntoSeries,
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
