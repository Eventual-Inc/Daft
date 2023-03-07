use std::sync::Arc;

use crate::{
    array::{BaseArray, DataArray},
    datatypes::{DaftNumericType, Float64Array, Utf8Array},
    error::{DaftError, DaftResult},
    kernels::utf8::add_utf8_arrays,
};

use super::DaftNumericAgg;

impl<T> DaftNumericAgg for &DataArray<T>
where
    T: DaftNumericType,
{
    type Output = DaftResult<DataArray<T>>;

    fn sum(&self) -> DaftResult<DataArray<T>> {
        let res = self.downcast().iter().fold(None, |acc, v| match v {
            Some(v) => match acc {
                Some(acc) => Some(acc + *v),
                None => Some(*v),
            },
            None => acc,
        });

        let scalar = res.unwrap();

        let arrow_array = arrow2::array::PrimitiveArray::<T::Native>::from_slice([scalar]);
        DataArray::new(self.field.clone(), Arc::new(arrow_array))
    }
}
