use crate::datatypes::DaftDataType;
use crate::{array::DataArray, datatypes::BooleanArray, error::DaftResult};
use arrow2::compute::if_then_else::if_then_else;

use crate::array::BaseArray;

impl BooleanArray {
    pub fn if_else<T: DaftDataType + 'static>(
        &self,
        if_true: &DataArray<T>,
        if_false: &DataArray<T>,
    ) -> DaftResult<DataArray<T>> {
        let result = if_then_else(self.downcast(), if_true.data(), if_false.data())?;
        DataArray::try_from((self.name(), result))
    }
}
