use arrow2::array::PrimitiveArray;

use crate::{
    array::data_array::{BaseArray, DataArray},
    datatypes::DaftNumericType,
};

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    // downcasts a DataArray<T> to an Arrow PrimitiveArray.
    pub fn downcast(&self) -> &PrimitiveArray<T::Native> {
        self.data().as_any().downcast_ref().unwrap()
    }
}
