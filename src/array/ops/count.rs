use std::sync::Arc;

use arrow2;

use crate::{array::DataArray, datatypes::*, error::DaftResult};

use super::DaftCountAggable;

impl<T> DaftCountAggable for &DataArray<T>
where
    T: DaftDataType,
{
    type Output = DaftResult<DataArray<UInt64Type>>;

    fn count(&self) -> Self::Output {
        let arrow_array = &self.data;
        let count = arrow_array.len() - arrow_array.null_count();
        let result_arrow_array = arrow2::array::PrimitiveArray::from([Some(count as u64)]);
        DataArray::<UInt64Type>::new(
            Arc::new(Field::new(self.field.name.clone(), DataType::UInt64)),
            Arc::new(result_arrow_array),
        )
    }
}
