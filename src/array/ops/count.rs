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
        let result_arrow_array = match arrow_array.len() {
            0 => arrow2::array::PrimitiveArray::from([]),
            _ => {
                let count = match arrow_array.validity() {
                    None => arrow_array.len(),
                    Some(bitmap) => arrow_array.len() - bitmap.unset_bits(),
                };
                arrow2::array::PrimitiveArray::from([Some(count as u64)])
            }
        };
        DataArray::<UInt64Type>::new(
            Arc::new(Field::new(self.field.name.clone(), DataType::UInt64)),
            Arc::new(result_arrow_array),
        )
    }
}
