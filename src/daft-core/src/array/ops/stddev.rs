use common_error::DaftResult;

use super::{DaftStddevAggable, GroupIndices};
use crate::{array::DataArray, datatypes::Float64Type};

impl DaftStddevAggable for DataArray<Float64Type> {
    type Output = DaftResult<Self>;

    fn stddev(&self) -> Self::Output {
        todo!("stddev")
    }

    fn grouped_stddev(&self, _: &GroupIndices) -> Self::Output {
        todo!("stddev")
    }
}
