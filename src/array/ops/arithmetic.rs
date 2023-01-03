use std::ops::{Add, Div, Mul, Rem, Sub};

use arrow2::{array::PrimitiveArray, compute::arithmetics::basic};

use crate::{array::data_array::DataArray, datatypes::DaftNumericType};

fn arithmetic_helper<T, Kernel, F>(
    lhs: &DataArray<T>,
    rhs: &DataArray<T>,
    kernel: Kernel,
    operation: F,
) -> DataArray<T>
where
    T: DaftNumericType,
    Kernel: Fn(&PrimitiveArray<T::Native>, &PrimitiveArray<T::Native>) -> PrimitiveArray<T::Native>,
    F: Fn(T::Native, T::Native) -> T::Native,
{
    let mut ca = match (lhs.len(), rhs.len()) {
        (a, b) if a == b => DataArray::from(kernel(lhs.downcast(), rhs.downcast()).boxed()),
        // broadcast right path
        (_, 1) => {
            let opt_rhs = rhs.get(0);
            match opt_rhs {
                None => DataArray::full_null(lhs.len()),
                Some(rhs) => lhs.apply(|lhs| operation(lhs, rhs)),
            }
        }
        (1, _) => {
            let opt_lhs = lhs.get(0);
            match opt_lhs {
                None => DataArray::full_null(rhs.len()),
                Some(lhs) => rhs.apply(|rhs| operation(lhs, rhs)),
            }
        }
        _ => panic!("Cannot apply operation on arrays of different lengths"),
    };
    // ca.rename(lhs.name());
    ca
}

impl<T> Add for &DataArray<T>
where
    T: DaftNumericType,
{
    type Output = DataArray<T>;
    fn add(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, basic::add, |l, r| l + r)
    }
}

impl<T> Sub for &DataArray<T>
where
    T: DaftNumericType,
{
    type Output = DataArray<T>;
    fn sub(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, basic::sub, |l, r| l - r)
    }
}

impl<T> Mul for &DataArray<T>
where
    T: DaftNumericType,
{
    type Output = DataArray<T>;
    fn mul(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, basic::mul, |l, r| l * r)
    }
}

impl<T> Div for &DataArray<T>
where
    T: DaftNumericType,
{
    type Output = DataArray<T>;
    fn div(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, basic::div, |l, r| l / r)
    }
}

impl<T> Rem for &DataArray<T>
where
    T: DaftNumericType,
{
    type Output = DataArray<T>;
    fn rem(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, basic::rem, |l, r| l % r)
    }
}
