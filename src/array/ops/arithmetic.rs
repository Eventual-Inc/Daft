use std::ops::{Add, Div, Mul, Rem, Sub};

use arrow2::{array::PrimitiveArray, compute::arithmetics::basic};

use crate::{array::data_array::DataArray, datatypes::DaftNumericType};
/// Helper function to perform arithmetic operations on a DataArray
/// Takes both Kernel (array x array operation) and operation (scalar x scalar) functions
/// The Kernel is used for when both arrays are non-unit length and the operation is used when broadcasting
/// This function is based from https://github.com/pola-rs/polars/blob/master/polars/polars-core/src/chunked_array/arithmetic.rs#L58
/// with the following license:
// Copyright (c) 2020 Ritchie Vink
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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
    let ca = match (lhs.len(), rhs.len()) {
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
