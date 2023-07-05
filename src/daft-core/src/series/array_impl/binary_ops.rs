use std::ops::{Add, Div, Mul, Rem, Sub};

use common_error::DaftResult;

use crate::{
    datatypes::{Float64Type, Utf8Type},
    series::series_like::SeriesLike,
    with_match_numeric_daft_types, DataType,
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

macro_rules! binary_op_default_impl {
    ($self:expr, $rhs:expr, $op:ident, $default_op:ident) => {{
        let output_type = ($self.data_type().$op($rhs.data_type()))?;
        let lhs = $self.into_series();
        $default_op(&lhs, $rhs, &output_type)
    }};
}

pub(crate) trait SeriesBinaryOps: SeriesLike {
    fn add(&self, rhs: &Series) -> DaftResult<Series> {
        binary_op_default_impl!(self, rhs, add, physical_add)
    }
    fn sub(&self, rhs: &Series) -> DaftResult<Series> {
        binary_op_default_impl!(self, rhs, sub, physical_sub)
    }
    fn mul(&self, rhs: &Series) -> DaftResult<Series> {
        binary_op_default_impl!(self, rhs, mul, physical_mul)
    }
    fn div(&self, rhs: &Series) -> DaftResult<Series> {
        binary_op_default_impl!(self, rhs, div, physical_div)
    }
    fn rem(&self, rhs: &Series) -> DaftResult<Series> {
        binary_op_default_impl!(self, rhs, rem, physical_rem)
    }
}

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

#[cfg(feature = "python")]
impl SeriesBinaryOps for ArrayWrapper<PythonArray> {}
impl SeriesBinaryOps for ArrayWrapper<NullArray> {}
impl SeriesBinaryOps for ArrayWrapper<BooleanArray> {}
impl SeriesBinaryOps for ArrayWrapper<BinaryArray> {}
impl SeriesBinaryOps for ArrayWrapper<Int8Array> {}
impl SeriesBinaryOps for ArrayWrapper<Int16Array> {}
impl SeriesBinaryOps for ArrayWrapper<Int32Array> {}
impl SeriesBinaryOps for ArrayWrapper<Int64Array> {}
impl SeriesBinaryOps for ArrayWrapper<UInt8Array> {}
impl SeriesBinaryOps for ArrayWrapper<UInt16Array> {}
impl SeriesBinaryOps for ArrayWrapper<UInt32Array> {}
impl SeriesBinaryOps for ArrayWrapper<UInt64Array> {}
impl SeriesBinaryOps for ArrayWrapper<Float32Array> {}
impl SeriesBinaryOps for ArrayWrapper<Float64Array> {}
impl SeriesBinaryOps for ArrayWrapper<Utf8Array> {}
impl SeriesBinaryOps for ArrayWrapper<FixedSizeListArray> {}
impl SeriesBinaryOps for ArrayWrapper<ListArray> {}
impl SeriesBinaryOps for ArrayWrapper<StructArray> {}
impl SeriesBinaryOps for ArrayWrapper<ExtensionArray> {}
impl SeriesBinaryOps for ArrayWrapper<DateArray> {}
impl SeriesBinaryOps for ArrayWrapper<DurationArray> {
    fn add(&self, rhs: &Series) -> DaftResult<Series> {
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
impl SeriesBinaryOps for ArrayWrapper<TimestampArray> {
    fn add(&self, rhs: &Series) -> DaftResult<Series> {
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
    fn sub(&self, rhs: &Series) -> DaftResult<Series> {
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
impl SeriesBinaryOps for ArrayWrapper<EmbeddingArray> {}
impl SeriesBinaryOps for ArrayWrapper<ImageArray> {}
impl SeriesBinaryOps for ArrayWrapper<FixedShapeImageArray> {}
