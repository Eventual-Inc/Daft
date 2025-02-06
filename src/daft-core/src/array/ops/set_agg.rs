use common_error::{DaftError, DaftResult};

use super::{DaftSetAggable, GroupIndices};
use crate::{
    array::{DataArray, FixedSizeListArray, ListArray, StructArray},
    datatypes::DaftArrowBackedType,
    series::IntoSeries,
};

macro_rules! impl_daft_set_agg {
    () => {
        type Output = DaftResult<ListArray>;

        fn distinct(&self) -> Self::Output {
            self.clone().into_series().distinct()
        }

        fn grouped_distinct(&self, groups: &GroupIndices) -> Self::Output {
            self.clone().into_series().grouped_distinct(groups)
        }
    };
}

impl<T> DaftSetAggable for DataArray<T>
where
    T: DaftArrowBackedType,
    Self: IntoSeries,
{
    impl_daft_set_agg!();
}

impl DaftSetAggable for ListArray {
    impl_daft_set_agg!();
}

impl DaftSetAggable for FixedSizeListArray {
    impl_daft_set_agg!();
}

impl DaftSetAggable for StructArray {
    impl_daft_set_agg!();
}

#[cfg(feature = "python")]
impl DaftSetAggable for crate::datatypes::PythonArray {
    type Output = DaftResult<Self>;

    fn distinct(&self) -> Self::Output {
        Err(DaftError::ValueError(
            "Cannot perform set aggregation on elements that are not hashable".to_string(),
        ))
    }

    fn grouped_distinct(&self, _: &GroupIndices) -> Self::Output {
        Err(DaftError::ValueError(
            "Cannot perform set aggregation on elements that are not hashable".to_string(),
        ))
    }
}
