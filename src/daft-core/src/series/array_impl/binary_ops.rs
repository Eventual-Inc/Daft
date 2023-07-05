use std::ops::{Add, Div, Mul, Rem, Sub};

use common_error::DaftResult;

use crate::{
    datatypes::{Float64Type, Utf8Type},
    series::series_like::SeriesLike,
    with_match_daft_types, with_match_numeric_daft_types, DataType,
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
#[cfg(feature = "python")]
macro_rules! py_binary_op_bool {
    ($lhs:expr, $rhs:expr, $pyoperator:expr) => {
        py_binary_op_utilfn!($lhs, $rhs, $pyoperator, "map_operator_arrow_semantics_bool")
    };
}

macro_rules! cast_downcast_op {
    ($lhs:expr, $rhs:expr, $ty_expr:expr, $ty_type:ty, $op:ident) => {{
        let lhs = $lhs.cast($ty_expr)?;
        let rhs = $rhs.cast($ty_expr)?;
        let lhs = lhs.downcast::<$ty_type>()?;
        let rhs = rhs.downcast::<$ty_type>()?;
        Ok(lhs.$op(rhs)?.into_series().rename(lhs.name()))
    }};
}

macro_rules! binary_op_unimplemented {
    ($lhs:expr, $op:expr, $rhs:expr, $output_ty:expr) => {
        unimplemented!(
            "No implementation for {} {} {} -> {}",
            $lhs.data_type(),
            $op,
            $rhs.data_type(),
            $output_ty,
        )
    };
}

macro_rules! py_numeric_binary_op {
    ($self:expr, $rhs:expr, $op:ident, $pyop:expr) => {{
        let output_type = ($self.data_type().$op($rhs.data_type()))?;
        let lhs = $self.into_series();
        use DataType::*;
        match &output_type {
            #[cfg(feature = "python")]
            Python => Ok(py_binary_op!(lhs, $rhs, $pyop)),
            output_type if output_type.is_numeric() => {
                with_match_numeric_daft_types!(output_type, |$T| {
                    cast_downcast_op!(lhs, $rhs, output_type, $T, $op)
                })
            }
            _ => binary_op_unimplemented!(lhs, $pyop, $rhs, output_type),
        }
    }};
}

pub(crate) trait SeriesBinaryOps: SeriesLike {
    fn add(&self, rhs: &Series) -> DaftResult<Series> {
        let output_type = (self.data_type().add(rhs.data_type()))?;
        let lhs = self.into_series();
        use DataType::*;
        match &output_type {
            #[cfg(feature = "python")]
            Python => Ok(py_binary_op!(lhs, rhs, "add")),
            Utf8 => cast_downcast_op!(lhs, rhs, &Utf8, Utf8Type, add),
            output_type if output_type.is_numeric() => {
                with_match_numeric_daft_types!(output_type, |$T| {
                    cast_downcast_op!(lhs, rhs, output_type, $T, add)
                })
            }
            _ => binary_op_unimplemented!(lhs, "+", rhs, output_type),
        }
    }
    fn sub(&self, rhs: &Series) -> DaftResult<Series> {
        py_numeric_binary_op!(self, rhs, sub, "sub")
    }
    fn mul(&self, rhs: &Series) -> DaftResult<Series> {
        py_numeric_binary_op!(self, rhs, mul, "mul")
    }
    fn div(&self, rhs: &Series) -> DaftResult<Series> {
        let output_type = (self.data_type().div(rhs.data_type()))?;
        let lhs = self.into_series();
        use DataType::*;
        match &output_type {
            #[cfg(feature = "python")]
            Python => Ok(py_binary_op!(lhs, rhs, "truediv")),
            Float64 => cast_downcast_op!(lhs, rhs, &Float64, Float64Type, div),
            _ => binary_op_unimplemented!(lhs, "/", rhs, output_type),
        }
    }
    fn rem(&self, rhs: &Series) -> DaftResult<Series> {
        py_numeric_binary_op!(self, rhs, rem, "rem")
    }
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
            _ => binary_op_unimplemented!(lhs, "+", rhs, output_type),
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
            _ => binary_op_unimplemented!(lhs, "+", rhs, output_type),
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
            _ => binary_op_unimplemented!(lhs, "-", rhs, output_type),
        }
    }
}
impl SeriesBinaryOps for ArrayWrapper<EmbeddingArray> {}
impl SeriesBinaryOps for ArrayWrapper<ImageArray> {}
impl SeriesBinaryOps for ArrayWrapper<FixedShapeImageArray> {}
