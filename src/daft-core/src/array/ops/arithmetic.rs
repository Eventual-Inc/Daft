use std::ops::{Add, Div, Mul, Rem, Sub};

use arrow2::{array::PrimitiveArray, compute::arithmetics::basic};

use crate::{
    array::{DataArray, FixedSizeListArray},
    datatypes::{DaftNumericType, Field, Float64Array, Int64Array, Utf8Array},
    kernels::utf8::add_utf8_arrays,
    DataType, Series,
};

use common_error::{DaftError, DaftResult};

use super::{as_arrow::AsArrow, full::FullNull};
/// Helper function to perform arithmetic operations on a DataArray
/// Takes both Kernel (array x array operation) and operation (scalar x scalar) functions
/// The Kernel is used for when both arrays are non-unit length and the operation is used when broadcasting
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
) -> DaftResult<DataArray<T>>
where
    T: DaftNumericType,
    Kernel: Fn(&PrimitiveArray<T::Native>, &PrimitiveArray<T::Native>) -> PrimitiveArray<T::Native>,
    F: Fn(T::Native, T::Native) -> T::Native,
{
    match (lhs.len(), rhs.len()) {
        (a, b) if a == b => Ok(DataArray::from((
            lhs.name(),
            Box::new(kernel(lhs.as_arrow(), rhs.as_arrow())),
        ))),
        // broadcast right path
        (_, 1) => {
            let opt_rhs = rhs.get(0);
            match opt_rhs {
                None => Ok(DataArray::full_null(lhs.name(), lhs.data_type(), lhs.len())),
                Some(rhs) => lhs.apply(|lhs| operation(lhs, rhs)),
            }
        }
        (1, _) => {
            let opt_lhs = lhs.get(0);
            match opt_lhs {
                None => Ok(DataArray::full_null(rhs.name(), lhs.data_type(), rhs.len())),
                // NOTE: naming logic here is wrong, and assigns the rhs name. However, renaming is handled at the Series level so this
                // error is obfuscated.
                Some(lhs) => rhs.apply(|rhs| operation(lhs, rhs)),
            }
        }
        (a, b) => Err(DaftError::ValueError(format!(
            "Cannot apply operation on arrays of different lengths: {a} vs {b}"
        ))),
    }
}

impl<T> Add for &DataArray<T>
where
    T: DaftNumericType,
    T::Native: basic::NativeArithmetics,
{
    type Output = DaftResult<DataArray<T>>;
    fn add(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, basic::add, |l, r| l + r)
    }
}

impl Add for &Utf8Array {
    type Output = DaftResult<Utf8Array>;
    fn add(self, rhs: Self) -> Self::Output {
        let result = Box::new(add_utf8_arrays(self.as_arrow(), rhs.as_arrow())?);
        Ok(Utf8Array::from((self.name(), result)))
    }
}
impl<T> Sub for &DataArray<T>
where
    T: DaftNumericType,
    T::Native: basic::NativeArithmetics,
{
    type Output = DaftResult<DataArray<T>>;
    fn sub(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, basic::sub, |l, r| l - r)
    }
}

impl<T> Mul for &DataArray<T>
where
    T: DaftNumericType,
    T::Native: basic::NativeArithmetics,
{
    type Output = DaftResult<DataArray<T>>;
    fn mul(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, basic::mul, |l, r| l * r)
    }
}

impl Div for &Float64Array {
    type Output = DaftResult<Float64Array>;
    fn div(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, basic::div, |l, r| l / r)
    }
}

impl Div for &Int64Array {
    type Output = DaftResult<Int64Array>;
    fn div(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, basic::div, |l, r| l / r)
    }
}

pub fn binary_with_nulls<T, F>(
    lhs: &PrimitiveArray<T>,
    rhs: &PrimitiveArray<T>,
    op: F,
) -> PrimitiveArray<T>
where
    T: arrow2::types::NativeType,
    F: Fn(T, T) -> T,
{
    if lhs.len() != rhs.len() {
        panic!("expected same length")
    }
    let values = lhs.iter().zip(rhs.iter()).map(|(l, r)| match (l, r) {
        (None, _) => None,
        (_, None) => None,
        (Some(l), Some(r)) => Some(op(*l, *r)),
    });
    unsafe { PrimitiveArray::<T>::from_trusted_len_iter_unchecked(values) }
}

fn rem_with_nulls<T>(lhs: &PrimitiveArray<T>, rhs: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: arrow2::types::NativeType + std::ops::Rem<Output = T>,
{
    binary_with_nulls(lhs, rhs, |a, b| a % b)
}

impl<T> Rem for &DataArray<T>
where
    T: DaftNumericType,
    T::Native: basic::NativeArithmetics,
{
    type Output = DaftResult<DataArray<T>>;
    fn rem(self, rhs: Self) -> Self::Output {
        if rhs.data().null_count() == 0 {
            arithmetic_helper(self, rhs, basic::rem, |l, r| l % r)
        } else {
            match (self.len(), rhs.len()) {
                (a, b) if a == b => Ok(DataArray::from((
                    self.name(),
                    Box::new(rem_with_nulls(self.as_arrow(), rhs.as_arrow())),
                ))),
                // broadcast right path
                (_, 1) => {
                    let opt_rhs = rhs.get(0);
                    match opt_rhs {
                        None => Ok(DataArray::full_null(
                            self.name(),
                            self.data_type(),
                            self.len(),
                        )),
                        Some(rhs) => self.apply(|lhs| lhs % rhs),
                    }
                }
                (1, _) => {
                    let opt_lhs = self.get(0);
                    Ok(match opt_lhs {
                        None => DataArray::full_null(rhs.name(), rhs.data_type(), rhs.len()),
                        Some(lhs) => {
                            let values_iter = rhs.as_arrow().iter().map(|v| v.map(|v| lhs % *v));
                            let arrow_array = unsafe {
                                PrimitiveArray::from_trusted_len_iter_unchecked(values_iter)
                            };
                            DataArray::from((self.name(), Box::new(arrow_array)))
                        }
                    })
                }
                (a, b) => Err(DaftError::ValueError(format!(
                    "Cannot apply operation on arrays of different lengths: {a} vs {b}"
                ))),
            }
        }
    }
}

fn fixed_sized_list_arithmetic_helper<Kernel>(
    lhs: &FixedSizeListArray,
    rhs: &FixedSizeListArray,
    kernel: Kernel,
) -> DaftResult<FixedSizeListArray>
where
    Kernel: Fn(&Series, &Series) -> DaftResult<Series>,
{
    assert_eq!(lhs.fixed_element_len(), rhs.fixed_element_len());

    let lhs_child: &Series = &lhs.flat_child;
    let rhs_child: &Series = &rhs.flat_child;
    let lhs_len = lhs.len();
    let rhs_len = rhs.len();

    let (result_child, validity) = match (lhs_len, rhs_len) {
        (a, b) if a == b => Ok((
            kernel(lhs_child, rhs_child)?,
            crate::utils::arrow::arrow_bitmap_and_helper(lhs.validity(), rhs.validity()),
        )),
        (l, 1) => {
            let validity = if rhs.is_valid(0) {
                lhs.validity().cloned()
            } else {
                Some(arrow2::bitmap::Bitmap::new_zeroed(l))
            };
            Ok((kernel(lhs_child, &rhs_child.repeat(lhs_len)?)?, validity))
        }
        (1, r) => {
            let validity = if lhs.is_valid(0) {
                rhs.validity().cloned()
            } else {
                Some(arrow2::bitmap::Bitmap::new_zeroed(r))
            };
            Ok((kernel(&lhs_child.repeat(lhs_len)?, rhs_child)?, validity))
        }
        (a, b) => Err(DaftError::ValueError(format!(
            "Cannot apply operation on arrays of different lengths: {a} vs {b}"
        ))),
    }?;

    let result_field = Field::new(
        lhs.name(),
        DataType::FixedSizeList(
            Box::new(result_child.data_type().clone()),
            lhs.fixed_element_len(),
        ),
    );
    Ok(FixedSizeListArray::new(
        result_field,
        result_child,
        validity,
    ))
}

impl Add for &FixedSizeListArray {
    type Output = DaftResult<FixedSizeListArray>;
    fn add(self, rhs: Self) -> Self::Output {
        fixed_sized_list_arithmetic_helper(self, rhs, |a, b| a + b)
    }
}

impl Mul for &FixedSizeListArray {
    type Output = DaftResult<FixedSizeListArray>;
    fn mul(self, rhs: Self) -> Self::Output {
        fixed_sized_list_arithmetic_helper(self, rhs, |a, b| a * b)
    }
}

impl Sub for &FixedSizeListArray {
    type Output = DaftResult<FixedSizeListArray>;
    fn sub(self, rhs: Self) -> Self::Output {
        fixed_sized_list_arithmetic_helper(self, rhs, |a, b| a - b)
    }
}

impl Div for &FixedSizeListArray {
    type Output = DaftResult<FixedSizeListArray>;
    fn div(self, rhs: Self) -> Self::Output {
        fixed_sized_list_arithmetic_helper(self, rhs, |a, b| a / b)
    }
}

impl Rem for &FixedSizeListArray {
    type Output = DaftResult<FixedSizeListArray>;
    fn rem(self, rhs: Self) -> Self::Output {
        fixed_sized_list_arithmetic_helper(self, rhs, |a, b| a % b)
    }
}
