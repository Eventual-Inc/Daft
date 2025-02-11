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

        fn set(&self) -> Self::Output {
            self.clone().into_series().set()
        }

        fn grouped_set(&self, groups: &GroupIndices) -> Self::Output {
            self.clone().into_series().grouped_set(groups)
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

    fn set(&self) -> Self::Output {
        Err(DaftError::ValueError(
            "Cannot perform set aggregation on elements that are not hashable".to_string(),
        ))
    }

    fn grouped_set(&self, _: &GroupIndices) -> Self::Output {
        Err(DaftError::ValueError(
            "Cannot perform set aggregation on elements that are not hashable".to_string(),
        ))
    }
}
