use arrow2::array::PrimitiveArray;

use crate::{
    array::data_array::{BaseArray, DataArray},
    datatypes::DaftNumericType,
};

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn apply<F>(&self, func: F) -> Self
    where
        F: Fn(T::Native) -> T::Native + Copy,
    {
        let arr: &PrimitiveArray<T::Native> = self.data().as_ref().as_any().downcast_ref().unwrap();
        let result_arr =
            PrimitiveArray::from_trusted_len_values_iter(arr.values_iter().map(|v| func(*v)));
        DataArray::from(
            result_arr
                .with_validity(arr.validity().clone().cloned())
                .boxed(),
        )
    }
}
