use std::{
    iter::zip,
    ops::{Add, Div, Mul, Rem, Sub},
    sync::Arc,
};

use arrow::buffer::NullBuffer;
use common_error::{DaftError, DaftResult};

use super::{as_arrow::AsArrow, full::FullNull};
use crate::{
    array::{DataArray, FixedSizeListArray},
    datatypes::{DaftNumericType, DaftPrimitiveType, DataType, Field, Utf8Array},
    kernels::{
        binary::{add_binary_arrays, add_fixed_size_binary_arrays},
        utf8::add_utf8_arrays,
    },
    prelude::{BinaryArray, Decimal128Array, FixedSizeBinaryArray},
    series::Series,
};
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

// TODO(desmond): Once DataArray stores arrow-rs arrays natively (instead of arrow2),
// `as_arrow()` becomes a free cast. At that point, the equal-length path in
// `arithmetic_helper` should delegate to arrow-rs kernels (e.g. `numeric::add_wrapping`)
// instead of a manual loop. Broadcasting and custom operations (Decimal128 scale
// adjustment, null-aware div/rem) will still need custom logic.
//
// Performance notes (benchmarked on aarch64 with tango-bench, n=1K..1M):
// - The `values().iter().zip().map(op).collect()` pattern auto-vectorizes: LLVM emits
//   4x-unrolled NEON `add.2d` instructions (8 i64s per iteration), identical to what
//   arrow-rs `binary()` in arrow-arith/src/arity.rs generates.
// - The arrow-rs kernel path (`as_arrow` -> kernel -> `from_arrow`) adds ~240ns of
//   conversion overhead (arrow2<->arrow-rs type wrapping) with no compute benefit, since
//   the kernel's inner loop is the same `zip().map().collect()` pattern.
// - At n>=100K all three paths (arrow2 kernel, arrow-rs kernel, scalar iter) converge to
//   the same throughput. At n=1K the conversion overhead dominates for the kernel paths.

/// Perform an element-wise arithmetic operation on two DataArrays, with broadcasting.
fn arithmetic_helper<T, F>(
    lhs: &DataArray<T>,
    rhs: &DataArray<T>,
    operation: F,
) -> DaftResult<DataArray<T>>
where
    T: DaftPrimitiveType,
    F: Fn(T::Native, T::Native) -> T::Native + Copy,
{
    match (lhs.len(), rhs.len()) {
        (a, b) if a == b => {
            let lhs_nulls = lhs.nulls().map(|v| v.clone().into());
            let rhs_nulls = rhs.nulls().map(|v| v.clone().into());
            let nulls = NullBuffer::union(lhs_nulls.as_ref(), rhs_nulls.as_ref());

            let lhs_values = lhs.values();
            let rhs_values = rhs.values();
            let iter = zip(lhs_values.iter(), rhs_values.iter()).map(|(l, r)| operation(*l, *r));

            DataArray::from_field_and_values(lhs.field.clone(), iter).with_nulls(nulls)
        }
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
                Some(scalar) => Ok(rhs.apply(|rhs| operation(scalar, rhs))?.rename(lhs.name())),
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
{
    type Output = DaftResult<DataArray<T>>;
    fn add(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, |l, r| l + r)
    }
}

impl Add for &Decimal128Array {
    type Output = DaftResult<Decimal128Array>;
    fn add(self, rhs: Self) -> Self::Output {
        assert_eq!(self.data_type(), rhs.data_type());
        arithmetic_helper(self, rhs, |l, r| l + r)
    }
}

impl Sub for &Decimal128Array {
    type Output = DaftResult<Decimal128Array>;
    fn sub(self, rhs: Self) -> Self::Output {
        assert_eq!(self.data_type(), rhs.data_type());
        arithmetic_helper(self, rhs, |l, r| l - r)
    }
}

impl Mul for &Decimal128Array {
    type Output = DaftResult<Decimal128Array>;
    fn mul(self, rhs: Self) -> Self::Output {
        assert_eq!(self.data_type(), rhs.data_type());

        let DataType::Decimal128(_, s) = self.data_type() else {
            unreachable!("This should always be a Decimal128")
        };
        let scale = 10i128.pow(*s as u32);
        arithmetic_helper(self, rhs, |l, r| (l * r) / scale)
    }
}

impl Add for &BinaryArray {
    type Output = DaftResult<BinaryArray>;
    fn add(self, rhs: Self) -> Self::Output {
        add_binary_arrays(self, rhs)
    }
}

impl Add for &FixedSizeBinaryArray {
    type Output = DaftResult<FixedSizeBinaryArray>;
    fn add(self, rhs: Self) -> Self::Output {
        add_fixed_size_binary_arrays(self, rhs)
    }
}

impl Add for &Utf8Array {
    type Output = DaftResult<Utf8Array>;
    fn add(self, rhs: Self) -> Self::Output {
        let result = Arc::new(add_utf8_arrays(&self.as_arrow()?, &rhs.as_arrow()?)?);

        Utf8Array::from_arrow(Field::new(self.name(), DataType::Utf8), result)
    }
}

impl<T> Sub for &DataArray<T>
where
    T: DaftNumericType,
{
    type Output = DaftResult<DataArray<T>>;
    fn sub(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, |l, r| l - r)
    }
}

impl<T> Mul for &DataArray<T>
where
    T: DaftNumericType,
{
    type Output = DaftResult<DataArray<T>>;
    fn mul(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, |l, r| l * r)
    }
}

/// Null-aware binary operation on equal-length DataArrays.
/// Unlike `arithmetic_helper`, this checks validity before computing each element,
/// avoiding panics from operations like division on garbage values at null positions.
fn null_aware_binary<T, F>(
    lhs: &DataArray<T>,
    rhs: &DataArray<T>,
    op: F,
) -> DaftResult<DataArray<T>>
where
    T: DaftPrimitiveType,
    F: Fn(T::Native, T::Native) -> T::Native,
{
    assert_eq!(lhs.len(), rhs.len(), "expected same length");
    let lhs_values = lhs.values();
    let rhs_values = rhs.values();
    let lhs_nulls = lhs.nulls().map(|v| v.clone().into());
    let rhs_nulls = rhs.nulls().map(|v| v.clone().into());
    let nulls = NullBuffer::union(lhs_nulls.as_ref(), rhs_nulls.as_ref());

    let iter = zip(lhs_values.iter(), rhs_values.iter())
        .enumerate()
        .map(|(i, (l, r))| {
            if nulls.as_ref().is_none_or(|nb| nb.is_valid(i)) {
                op(*l, *r)
            } else {
                *l // placeholder; position is null
            }
        });

    DataArray::from_field_and_values(lhs.field.clone(), iter).with_nulls(nulls)
}

/// Helper for broadcast-left (lhs=scalar) with null-aware rhs iteration.
/// Used by div/rem when rhs has nulls that might contain garbage zeros.
fn broadcast_left_null_aware<T, F>(
    lhs: &DataArray<T>,
    rhs: &DataArray<T>,
    lhs_val: T::Native,
    op: F,
) -> DaftResult<DataArray<T>>
where
    T: DaftPrimitiveType,
    F: Fn(T::Native, T::Native) -> T::Native,
{
    let rhs_values = rhs.values();
    let rhs_nulls = rhs.nulls();
    let iter = rhs_values.iter().enumerate().map(|(i, r)| {
        if rhs_nulls.is_none_or(|nb| nb.is_valid(i)) {
            op(lhs_val, *r)
        } else {
            lhs_val // placeholder; position is null
        }
    });
    DataArray::from_field_and_values(lhs.field.clone(), iter)
        .with_nulls(rhs.nulls().cloned().map(Into::into))
}

impl<T> Rem for &DataArray<T>
where
    T: DaftNumericType,
{
    type Output = DaftResult<DataArray<T>>;
    fn rem(self, rhs: Self) -> Self::Output {
        if rhs.data().null_count() == 0 {
            arithmetic_helper(self, rhs, |l, r| l % r)
        } else {
            match (self.len(), rhs.len()) {
                (a, b) if a == b => null_aware_binary(self, rhs, |a, b| a % b),
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
                        Some(lhs_val) => {
                            broadcast_left_null_aware(self, rhs, lhs_val, |l, r| l % r)?
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

impl<T> Div for &DataArray<T>
where
    T: DaftNumericType,
{
    type Output = DaftResult<DataArray<T>>;
    fn div(self, rhs: Self) -> Self::Output {
        if rhs.data().null_count() == 0 {
            arithmetic_helper(self, rhs, |l, r| l / r)
        } else {
            match (self.len(), rhs.len()) {
                (a, b) if a == b => null_aware_binary(self, rhs, |a, b| a / b),
                // broadcast right path
                (_, 1) => {
                    let opt_rhs = rhs.get(0);
                    match opt_rhs {
                        None => Ok(DataArray::full_null(
                            self.name(),
                            self.data_type(),
                            self.len(),
                        )),
                        Some(rhs) => self.apply(|lhs| lhs / rhs),
                    }
                }
                (1, _) => {
                    let opt_lhs = self.get(0);
                    Ok(match opt_lhs {
                        None => DataArray::full_null(rhs.name(), rhs.data_type(), rhs.len()),
                        Some(lhs_val) => {
                            broadcast_left_null_aware(self, rhs, lhs_val, |l, r| l / r)?
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

impl Div for &Decimal128Array {
    type Output = DaftResult<Decimal128Array>;
    fn div(self, rhs: Self) -> Self::Output {
        assert_eq!(self.data_type(), rhs.data_type());
        let DataType::Decimal128(_, s) = self.data_type() else {
            unreachable!("This should always be a Decimal128")
        };
        let scale = 10i128.pow(*s as u32);

        if rhs.data().null_count() == 0 {
            arithmetic_helper(self, rhs, |l, r| (l * scale) / r)
        } else {
            match (self.len(), rhs.len()) {
                (a, b) if a == b => null_aware_binary(self, rhs, |l, r| (l * scale) / r),
                // broadcast right path
                (_, 1) => {
                    let opt_rhs = rhs.get(0);
                    match opt_rhs {
                        None => Ok(DataArray::full_null(
                            self.name(),
                            self.data_type(),
                            self.len(),
                        )),
                        Some(rhs) => self.apply(|lhs| (lhs * scale) / rhs),
                    }
                }
                (1, _) => {
                    let opt_lhs = self.get(0);
                    Ok(match opt_lhs {
                        None => DataArray::full_null(rhs.name(), rhs.data_type(), rhs.len()),
                        Some(lhs_val) => {
                            broadcast_left_null_aware(self, rhs, lhs_val, |l, r| (l * scale) / r)?
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

    let (result_child, nulls) = match (lhs_len, rhs_len) {
        (a, b) if a == b => Ok((
            kernel(lhs_child, rhs_child)?,
            NullBuffer::union(lhs.nulls(), rhs.nulls()),
        )),
        (l, 1) => {
            let nulls = if rhs.is_valid(0) {
                lhs.nulls().cloned()
            } else {
                Some(NullBuffer::new_null(l))
            };
            Ok((kernel(lhs_child, &rhs_child.repeat(lhs_len)?)?, nulls))
        }
        (1, r) => {
            let nulls = if lhs.is_valid(0) {
                rhs.nulls().cloned()
            } else {
                Some(NullBuffer::new_null(r))
            };
            Ok((kernel(&lhs_child.repeat(lhs_len)?, rhs_child)?, nulls))
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
    Ok(FixedSizeListArray::new(result_field, result_child, nulls))
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
