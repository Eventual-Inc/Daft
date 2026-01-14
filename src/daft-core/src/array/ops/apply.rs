use std::iter::zip;

use common_error::{DaftError, DaftResult};

use super::full::FullNull;
use crate::{
    array::DataArray,
    datatypes::{DaftNumericType, DaftPrimitiveType},
};

impl<T> DataArray<T>
where
    T: DaftPrimitiveType,
{
    // applies a native function to a numeric DataArray maintaining validity of the source array.
    pub fn apply<F>(&self, func: F) -> DaftResult<Self>
    where
        F: Fn(T::Native) -> T::Native + Copy,
    {
        let values = self.values();

        let iter = values.iter().map(|v| func(*v));

        Self::from_values_iter(self.field.clone(), iter)
            .with_nulls(self.nulls().cloned().map(Into::into))
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
                let lhs_nulls = self.nulls().map(|v| v.clone().into());
                let rhs_nulls = rhs.nulls().map(|v| v.clone().into());

                let nulls =
                    daft_arrow::buffer::NullBuffer::union(lhs_nulls.as_ref(), rhs_nulls.as_ref());
                let values = self.values();
                let rhs_values = rhs.values();

                let iter = zip(values.iter(), rhs_values.iter()).map(|(a, b)| func(*a, *b));
                Self::from_values_iter(self.field.clone(), iter).with_nulls(nulls)
            }
            (l_size, 1) => {
                if let Some(value) = rhs.get(0) {
                    self.apply(|v| func(v, value))
                } else {
                    Ok(Self::full_null(self.name(), self.data_type(), l_size))
                }
            }
            (1, r_size) => {
                if let Some(value) = self.get(0) {
                    let rhs_values = rhs.values();
                    let iter = rhs_values.iter().map(|v| func(value, *v));
                    Self::from_values_iter(self.field.clone(), iter)
                        .with_nulls(rhs.nulls().cloned().map(Into::into))
                } else {
                    Ok(Self::full_null(self.name(), self.data_type(), r_size))
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
