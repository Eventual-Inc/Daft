use std::ops::{Add, Div, Mul, Rem, Sub};

use common_error::{DaftError, DaftResult};

use crate::{
    datatypes::{Float64Type, Utf8Type},
    series::series_like::SeriesLike,
    with_match_numeric_and_utf_daft_types, with_match_numeric_daft_types, DataType,
};

use crate::datatypes::{
    BinaryArray, BooleanArray, ExtensionArray, FixedSizeListArray, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, ListArray, NullArray, StructArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array, Utf8Array,
};

use crate::datatypes::logical::{
    DateArray, DurationArray, EmbeddingArray, FixedShapeImageArray, ImageArray, TimestampArray,
};

use super::{ArrayWrapper, IntoSeries, Series};

#[cfg(feature = "python")]
use crate::{datatypes::PythonArray, series::ops::py_binary_op_utilfn};

#[cfg(feature = "python")]
macro_rules! py_binary_op {
    ($lhs:expr, $rhs:expr, $pyoperator:expr) => {
        py_binary_op_utilfn!($lhs, $rhs, $pyoperator, "map_operator_arrow_semantics")
    };
}

macro_rules! py_numeric_binary_op {
    ($op:ident, $pyop:expr, $lhs:expr, $rhs:expr, $output_ty:expr) => {{
        use DataType::*;
        match $output_ty {
            #[cfg(feature = "python")]
            Python => Ok(py_binary_op!($lhs, $rhs, $pyop)),
            output_type if output_type.is_numeric() => {
                let lhs = $lhs.cast(&output_type)?;
                let rhs = $rhs.cast(&output_type)?;
                with_match_numeric_daft_types!(output_type, |$T| {
                    let lhs = lhs.downcast::<$T>()?;
                    let rhs = rhs.downcast::<$T>()?;
                    Ok(lhs.$op(rhs)?.into_series().rename(lhs.name()))
                })
            }
            _ => panic!(
                "No implementation for {} {} {} -> {}",
                $lhs.data_type(),
                $pyop,
                $rhs.data_type(),
                $output_ty,
            ),
        }
    }};
}

fn physical_add(lhs: &Series, rhs: &Series, output_type: &DataType) -> DaftResult<Series> {
    use DataType::*;
    match output_type {
        Utf8 => {
            let lhs = lhs.cast(&Utf8)?;
            let rhs = rhs.cast(&Utf8)?;
            let lhs = lhs.downcast::<Utf8Type>()?;
            let rhs = rhs.downcast::<Utf8Type>()?;
            Ok(lhs.add(rhs)?.into_series().rename(lhs.name()))
        }
        _ => py_numeric_binary_op!(add, "add", lhs, rhs, output_type),
    }
}

fn physical_sub(lhs: &Series, rhs: &Series, output_type: &DataType) -> DaftResult<Series> {
    py_numeric_binary_op!(sub, "sub", lhs, rhs, output_type)
}

fn physical_mul(lhs: &Series, rhs: &Series, output_type: &DataType) -> DaftResult<Series> {
    py_numeric_binary_op!(mul, "mul", lhs, rhs, output_type)
}

fn physical_div(lhs: &Series, rhs: &Series, output_type: &DataType) -> DaftResult<Series> {
    use DataType::*;
    match output_type {
        #[cfg(feature = "python")]
        Python => Ok(py_binary_op!(lhs, rhs, "truediv")),
        Float64 => {
            let lhs = lhs.cast(&Float64)?;
            let rhs = rhs.cast(&Float64)?;
            let lhs = lhs.downcast::<Float64Type>()?;
            let rhs = rhs.downcast::<Float64Type>()?;
            Ok(lhs.div(rhs)?.into_series().rename(lhs.name()))
        }
        _ => panic!(
            "No implementation for {} / {} -> {}",
            lhs.data_type(),
            rhs.data_type(),
            output_type,
        ),
    }
}

fn physical_rem(lhs: &Series, rhs: &Series, output_type: &DataType) -> DaftResult<Series> {
    py_numeric_binary_op!(rem, "mod", lhs, rhs, output_type)
}

impl Add<&Series> for &ArrayWrapper<TimestampArray> {
    type Output = DaftResult<Series>;

    fn add(self, rhs: &Series) -> Self::Output {
        use DataType::*;
        let output_type = (self.data_type() + rhs.data_type())?;
        let lhs = self.0.clone().into_series();
        match rhs.data_type() {
            Duration(..) => {
                let lhs = lhs.as_physical()?;
                let rhs = rhs.as_physical()?;
                let physical_result = lhs.add(rhs)?;
                physical_result.cast(&output_type)
            }
            _ => physical_add(&lhs, rhs, &output_type),
        }
    }
}

impl Sub<&Series> for &ArrayWrapper<TimestampArray> {
    type Output = DaftResult<Series>;

    fn sub(self, rhs: &Series) -> Self::Output {
        use DataType::*;
        let output_type = (self.data_type() - rhs.data_type())?;
        let lhs = self.0.clone().into_series();
        match rhs.data_type() {
            Duration(..) => {
                let lhs = lhs.as_physical()?;
                let rhs = rhs.as_physical()?;
                let physical_result = lhs.sub(rhs)?;
                physical_result.cast(&output_type)
            }
            _ => physical_sub(&lhs, rhs, &output_type),
        }
    }
}

impl Add<&Series> for &ArrayWrapper<DurationArray> {
    type Output = DaftResult<Series>;

    fn add(self, rhs: &Series) -> Self::Output {
        use DataType::*;
        let output_type = (self.data_type() + rhs.data_type())?;
        let lhs = self.0.clone().into_series();
        match rhs.data_type() {
            Timestamp(..) => {
                let lhs = lhs.as_physical()?;
                let rhs = rhs.as_physical()?;
                let physical_result = lhs.add(rhs)?;
                physical_result.cast(&output_type)
            }
            _ => physical_add(&lhs, rhs, &output_type),
        }
    }
}

macro_rules! impl_default_arithmetic {
    ($arrayT:ty, $trait:ident, $op:ident, $default_op:ident) => {
        impl $trait<&Series> for &ArrayWrapper<$arrayT> {
            type Output = DaftResult<Series>;

            fn $op(self, rhs: &Series) -> Self::Output {
                let output_type = (self.data_type().$op(rhs.data_type()))?;
                let lhs = self.0.clone().into_series();
                $default_op(&lhs, rhs, &output_type)
            }
        }
    };
}

#[cfg(feature = "python")]
impl_default_arithmetic!(PythonArray, Add, add, physical_add);
#[cfg(feature = "python")]
impl_default_arithmetic!(PythonArray, Sub, sub, physical_sub);
#[cfg(feature = "python")]
impl_default_arithmetic!(PythonArray, Mul, mul, physical_mul);
#[cfg(feature = "python")]
impl_default_arithmetic!(PythonArray, Div, div, physical_div);
#[cfg(feature = "python")]
impl_default_arithmetic!(PythonArray, Rem, rem, physical_rem);

impl_default_arithmetic!(NullArray, Add, add, physical_add);
impl_default_arithmetic!(NullArray, Sub, sub, physical_sub);
impl_default_arithmetic!(NullArray, Mul, mul, physical_mul);
impl_default_arithmetic!(NullArray, Div, div, physical_div);
impl_default_arithmetic!(NullArray, Rem, rem, physical_rem);

impl_default_arithmetic!(BooleanArray, Add, add, physical_add);
impl_default_arithmetic!(BooleanArray, Sub, sub, physical_sub);
impl_default_arithmetic!(BooleanArray, Mul, mul, physical_mul);
impl_default_arithmetic!(BooleanArray, Div, div, physical_div);
impl_default_arithmetic!(BooleanArray, Rem, rem, physical_rem);

impl_default_arithmetic!(BinaryArray, Add, add, physical_add);
impl_default_arithmetic!(BinaryArray, Sub, sub, physical_sub);
impl_default_arithmetic!(BinaryArray, Mul, mul, physical_mul);
impl_default_arithmetic!(BinaryArray, Div, div, physical_div);
impl_default_arithmetic!(BinaryArray, Rem, rem, physical_rem);

impl_default_arithmetic!(Int8Array, Add, add, physical_add);
impl_default_arithmetic!(Int8Array, Sub, sub, physical_sub);
impl_default_arithmetic!(Int8Array, Mul, mul, physical_mul);
impl_default_arithmetic!(Int8Array, Div, div, physical_div);
impl_default_arithmetic!(Int8Array, Rem, rem, physical_rem);

impl_default_arithmetic!(Int16Array, Add, add, physical_add);
impl_default_arithmetic!(Int16Array, Sub, sub, physical_sub);
impl_default_arithmetic!(Int16Array, Mul, mul, physical_mul);
impl_default_arithmetic!(Int16Array, Div, div, physical_div);
impl_default_arithmetic!(Int16Array, Rem, rem, physical_rem);

impl_default_arithmetic!(Int32Array, Add, add, physical_add);
impl_default_arithmetic!(Int32Array, Sub, sub, physical_sub);
impl_default_arithmetic!(Int32Array, Mul, mul, physical_mul);
impl_default_arithmetic!(Int32Array, Div, div, physical_div);
impl_default_arithmetic!(Int32Array, Rem, rem, physical_rem);

impl_default_arithmetic!(Int64Array, Add, add, physical_add);
impl_default_arithmetic!(Int64Array, Sub, sub, physical_sub);
impl_default_arithmetic!(Int64Array, Mul, mul, physical_mul);
impl_default_arithmetic!(Int64Array, Div, div, physical_div);
impl_default_arithmetic!(Int64Array, Rem, rem, physical_rem);

impl_default_arithmetic!(UInt8Array, Add, add, physical_add);
impl_default_arithmetic!(UInt8Array, Sub, sub, physical_sub);
impl_default_arithmetic!(UInt8Array, Mul, mul, physical_mul);
impl_default_arithmetic!(UInt8Array, Div, div, physical_div);
impl_default_arithmetic!(UInt8Array, Rem, rem, physical_rem);

impl_default_arithmetic!(UInt16Array, Add, add, physical_add);
impl_default_arithmetic!(UInt16Array, Sub, sub, physical_sub);
impl_default_arithmetic!(UInt16Array, Mul, mul, physical_mul);
impl_default_arithmetic!(UInt16Array, Div, div, physical_div);
impl_default_arithmetic!(UInt16Array, Rem, rem, physical_rem);

impl_default_arithmetic!(UInt32Array, Add, add, physical_add);
impl_default_arithmetic!(UInt32Array, Sub, sub, physical_sub);
impl_default_arithmetic!(UInt32Array, Mul, mul, physical_mul);
impl_default_arithmetic!(UInt32Array, Div, div, physical_div);
impl_default_arithmetic!(UInt32Array, Rem, rem, physical_rem);

impl_default_arithmetic!(UInt64Array, Add, add, physical_add);
impl_default_arithmetic!(UInt64Array, Sub, sub, physical_sub);
impl_default_arithmetic!(UInt64Array, Mul, mul, physical_mul);
impl_default_arithmetic!(UInt64Array, Div, div, physical_div);
impl_default_arithmetic!(UInt64Array, Rem, rem, physical_rem);

impl_default_arithmetic!(Float32Array, Add, add, physical_add);
impl_default_arithmetic!(Float32Array, Sub, sub, physical_sub);
impl_default_arithmetic!(Float32Array, Mul, mul, physical_mul);
impl_default_arithmetic!(Float32Array, Div, div, physical_div);
impl_default_arithmetic!(Float32Array, Rem, rem, physical_rem);

impl_default_arithmetic!(Float64Array, Add, add, physical_add);
impl_default_arithmetic!(Float64Array, Sub, sub, physical_sub);
impl_default_arithmetic!(Float64Array, Mul, mul, physical_mul);
impl_default_arithmetic!(Float64Array, Div, div, physical_div);
impl_default_arithmetic!(Float64Array, Rem, rem, physical_rem);

impl_default_arithmetic!(Utf8Array, Add, add, physical_add);
impl_default_arithmetic!(Utf8Array, Sub, sub, physical_sub);
impl_default_arithmetic!(Utf8Array, Mul, mul, physical_mul);
impl_default_arithmetic!(Utf8Array, Div, div, physical_div);
impl_default_arithmetic!(Utf8Array, Rem, rem, physical_rem);

impl_default_arithmetic!(FixedSizeListArray, Add, add, physical_add);
impl_default_arithmetic!(FixedSizeListArray, Sub, sub, physical_sub);
impl_default_arithmetic!(FixedSizeListArray, Mul, mul, physical_mul);
impl_default_arithmetic!(FixedSizeListArray, Div, div, physical_div);
impl_default_arithmetic!(FixedSizeListArray, Rem, rem, physical_rem);

impl_default_arithmetic!(ListArray, Add, add, physical_add);
impl_default_arithmetic!(ListArray, Sub, sub, physical_sub);
impl_default_arithmetic!(ListArray, Mul, mul, physical_mul);
impl_default_arithmetic!(ListArray, Div, div, physical_div);
impl_default_arithmetic!(ListArray, Rem, rem, physical_rem);

impl_default_arithmetic!(StructArray, Add, add, physical_add);
impl_default_arithmetic!(StructArray, Sub, sub, physical_sub);
impl_default_arithmetic!(StructArray, Mul, mul, physical_mul);
impl_default_arithmetic!(StructArray, Div, div, physical_div);
impl_default_arithmetic!(StructArray, Rem, rem, physical_rem);

impl_default_arithmetic!(ExtensionArray, Add, add, physical_add);
impl_default_arithmetic!(ExtensionArray, Sub, sub, physical_sub);
impl_default_arithmetic!(ExtensionArray, Mul, mul, physical_mul);
impl_default_arithmetic!(ExtensionArray, Div, div, physical_div);
impl_default_arithmetic!(ExtensionArray, Rem, rem, physical_rem);

impl_default_arithmetic!(DateArray, Add, add, physical_add);
impl_default_arithmetic!(DateArray, Sub, sub, physical_sub);
impl_default_arithmetic!(DateArray, Mul, mul, physical_mul);
impl_default_arithmetic!(DateArray, Div, div, physical_div);
impl_default_arithmetic!(DateArray, Rem, rem, physical_rem);

impl_default_arithmetic!(DurationArray, Sub, sub, physical_sub);
impl_default_arithmetic!(DurationArray, Mul, mul, physical_mul);
impl_default_arithmetic!(DurationArray, Div, div, physical_div);
impl_default_arithmetic!(DurationArray, Rem, rem, physical_rem);

impl_default_arithmetic!(TimestampArray, Mul, mul, physical_mul);
impl_default_arithmetic!(TimestampArray, Div, div, physical_div);
impl_default_arithmetic!(TimestampArray, Rem, rem, physical_rem);

impl_default_arithmetic!(EmbeddingArray, Add, add, physical_add);
impl_default_arithmetic!(EmbeddingArray, Sub, sub, physical_sub);
impl_default_arithmetic!(EmbeddingArray, Mul, mul, physical_mul);
impl_default_arithmetic!(EmbeddingArray, Div, div, physical_div);
impl_default_arithmetic!(EmbeddingArray, Rem, rem, physical_rem);

impl_default_arithmetic!(ImageArray, Add, add, physical_add);
impl_default_arithmetic!(ImageArray, Sub, sub, physical_sub);
impl_default_arithmetic!(ImageArray, Mul, mul, physical_mul);
impl_default_arithmetic!(ImageArray, Div, div, physical_div);
impl_default_arithmetic!(ImageArray, Rem, rem, physical_rem);

impl_default_arithmetic!(FixedShapeImageArray, Add, add, physical_add);
impl_default_arithmetic!(FixedShapeImageArray, Sub, sub, physical_sub);
impl_default_arithmetic!(FixedShapeImageArray, Mul, mul, physical_mul);
impl_default_arithmetic!(FixedShapeImageArray, Div, div, physical_div);
impl_default_arithmetic!(FixedShapeImageArray, Rem, rem, physical_rem);
