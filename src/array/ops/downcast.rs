use arrow2::array::PrimitiveArray;

use crate::{
    array::data_array::{BaseArray, DataArray},
    datatypes::DaftNumericType,
};

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn downcast(&self) -> &PrimitiveArray<T::Native> {
        self.data().as_ref().as_any().downcast_ref().unwrap()
    }
}
