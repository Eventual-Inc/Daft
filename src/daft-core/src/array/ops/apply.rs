use std::iter::zip;

use arrow2::array::PrimitiveArray;

use crate::{array::DataArray, datatypes::DaftNumericType, utils::arrow::arrow_bitmap_and_helper};

use common_error::{DaftError, DaftResult};

use super::full::FullNull;

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    // applies a native function to a numeric DataArray maintaining validity of the source array.
    pub fn apply<F>(&self, func: F) -> DaftResult<Self>
    where
        F: Fn(T::Native) -> T::Native + Copy,
    {
        let arr: &PrimitiveArray<T::Native> = self.data().as_any().downcast_ref().unwrap();
        let result_arr =
            PrimitiveArray::from_trusted_len_values_iter(arr.values_iter().map(|v| func(*v)))
                .with_validity(arr.validity().cloned());

        Ok(DataArray::from((self.name(), Box::new(result_arr))))
    }

    // applies a native binary function to two DataArrays, maintaining validity.
    // If the two arrays have the same length, applies row-by-row.
    // If one of the arrays has length 1, treats it as if the value were repeated.
    // Note: the name of the output array takes the name of the left hand side.
    pub fn binary_apply<F, R>(&self, rhs: &DataArray<R>, func: F) -> DaftResult<Self>
    where
        R: DaftNumericType,
        F: Fn(T::Native, R::Native) -> T::Native + Copy,
    {
        match (self.len(), rhs.len()) {
            (x, y) if x == y => {
                let lhs_arr: &PrimitiveArray<T::Native> =
                    self.data().as_any().downcast_ref().unwrap();
                let rhs_arr: &PrimitiveArray<R::Native> =
                    rhs.data().as_any().downcast_ref().unwrap();

                let validity = arrow_bitmap_and_helper(lhs_arr.validity(), rhs_arr.validity());
                let result_arr = PrimitiveArray::from_trusted_len_values_iter(
                    zip(lhs_arr.values_iter(), rhs_arr.values_iter()).map(|(a, b)| func(*a, *b)),
                )
                .with_validity(validity);
                Ok(DataArray::from((self.name(), Box::new(result_arr))))
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.apply(|v| func(v, value))
                } else {
                    Ok(DataArray::<T>::full_null(
                        self.name(),
                        self.data_type(),
                        l_size,
                    ))
                }
            }
            (1, r_size) => {
                let rhs_arr: &PrimitiveArray<R::Native> =
                    rhs.data().as_any().downcast_ref().unwrap();
                if let Some(value) = self.get(0) {
                    let result_arr = PrimitiveArray::from_trusted_len_values_iter(
                        rhs_arr.values_iter().map(|v| func(value, *v)),
                    )
                    .with_validity(rhs_arr.validity().cloned());
                    Ok(DataArray::from((self.name(), Box::new(result_arr))))
                } else {
                    Ok(DataArray::<T>::full_null(
                        self.name(),
                        self.data_type(),
                        r_size,
                    ))
                }
            }
            (l, r) => Err(DaftError::ValueError(format!(
                "trying to operate on different length arrays: {}: {l} vs {}: {r}",
                self.name(),
                rhs.name()
            ))),
        }
    }
}
