use common_error::DaftResult;

use crate::{array::DataArray, datatypes::DaftPhysicalType};

use super::{DaftDistinctAggable, GroupIndices};

impl<T> DaftDistinctAggable for &DataArray<T>
where
    T: DaftPhysicalType,
{
    type Output = DaftResult<DataArray<T>>;

    fn distinct(&self) -> Self::Output {
        todo!()
    }

    fn grouped_distinct(&self, _: &GroupIndices) -> Self::Output {
        todo!()
    }
}
