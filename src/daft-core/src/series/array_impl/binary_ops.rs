use std::ops::{Add, Div, Mul, Rem, Sub};

use common_error::DaftResult;

use crate::{
    array::{
        ops::{DaftCompare, DaftLogical},
        FixedSizeListArray, ListArray, StructArray,
    },
    datatypes::{logical::Decimal128Array, Int128Array},
    series::series_like::SeriesLike,
    with_match_comparable_daft_types, with_match_numeric_daft_types, DataType,
};

use crate::datatypes::logical::{
    DateArray, DurationArray, EmbeddingArray, FixedShapeImageArray, FixedShapeTensorArray,
    ImageArray, TensorArray, TimestampArray,
};
use crate::datatypes::{
    BinaryArray, BooleanArray, ExtensionArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, NullArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array, Utf8Array,
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
        lhs.$op(rhs)
    }};
}

macro_rules! cast_downcast_op_into_series {
    ($lhs:expr, $rhs:expr, $ty_expr:expr, $ty_type:ty, $op:ident) => {{
        Ok(cast_downcast_op!($lhs, $rhs, $ty_expr, $ty_type, $op)?
            .into_series()
            .rename($lhs.name()))
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
                    cast_downcast_op_into_series!(
                        lhs,
                        $rhs,
                        output_type,
                        <$T as DaftDataType>::ArrayType,
                        $op
                    )
                })
            }
            _ => binary_op_unimplemented!(lhs, $pyop, $rhs, output_type),
        }
    }};
}

macro_rules! physical_logic_op {
    ($self:expr, $rhs:expr, $op:ident, $pyop:expr) => {{
        let output_type = ($self.data_type().logical_op($rhs.data_type()))?;
        let lhs = $self.into_series();
        use DataType::*;
        if let Boolean = output_type {
            match (&lhs.data_type(), &$rhs.data_type()) {
                #[cfg(feature = "python")]
                (Python, _) | (_, Python) => py_binary_op_bool!(lhs, $rhs, $pyop)
                    .downcast::<BooleanArray>()
                    .cloned(),
                _ => cast_downcast_op!(lhs, $rhs, &Boolean, BooleanArray, $op),
            }
        } else {
            unreachable!()
        }
    }};
}

macro_rules! physical_compare_op {
    ($self:expr, $rhs:expr, $op:ident, $pyop:expr) => {{
        let (output_type, comp_type) = ($self.data_type().comparison_op($rhs.data_type()))?;
        let lhs = $self.into_series();
        use DataType::*;
        if let Boolean = output_type {
            match comp_type {
                #[cfg(feature = "python")]
                Python => py_binary_op_bool!(lhs, $rhs, $pyop)
                    .downcast::<BooleanArray>()
                    .cloned(),
                _ => with_match_comparable_daft_types!(comp_type, |$T| {
                    cast_downcast_op!(lhs, $rhs, &comp_type, <$T as DaftDataType>::ArrayType, $op)
                }),
            }
        } else {
            unreachable!()
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
            Utf8 => cast_downcast_op_into_series!(lhs, rhs, &Utf8, Utf8Array, add),
            output_type if output_type.is_numeric() => {
                with_match_numeric_daft_types!(output_type, |$T| {
                    cast_downcast_op_into_series!(lhs, rhs, output_type, <$T as DaftDataType>::ArrayType, add)
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
            Float64 => cast_downcast_op_into_series!(lhs, rhs, &Float64, Float64Array, div),
            _ => binary_op_unimplemented!(lhs, "/", rhs, output_type),
        }
    }
    fn rem(&self, rhs: &Series) -> DaftResult<Series> {
        py_numeric_binary_op!(self, rhs, rem, "mod")
    }
    fn and(&self, rhs: &Series) -> DaftResult<BooleanArray> {
        physical_logic_op!(self, rhs, and, "and_")
    }
    fn or(&self, rhs: &Series) -> DaftResult<BooleanArray> {
        physical_logic_op!(self, rhs, or, "or_")
    }
    fn xor(&self, rhs: &Series) -> DaftResult<BooleanArray> {
        physical_logic_op!(self, rhs, xor, "xor")
    }
    fn equal(&self, rhs: &Series) -> DaftResult<BooleanArray> {
        physical_compare_op!(self, rhs, equal, "eq")
    }
    fn not_equal(&self, rhs: &Series) -> DaftResult<BooleanArray> {
        physical_compare_op!(self, rhs, not_equal, "ne")
    }
    fn lt(&self, rhs: &Series) -> DaftResult<BooleanArray> {
        physical_compare_op!(self, rhs, lt, "lt")
    }
    fn lte(&self, rhs: &Series) -> DaftResult<BooleanArray> {
        physical_compare_op!(self, rhs, lte, "le")
    }
    fn gt(&self, rhs: &Series) -> DaftResult<BooleanArray> {
        physical_compare_op!(self, rhs, gt, "gt")
    }
    fn gte(&self, rhs: &Series) -> DaftResult<BooleanArray> {
        physical_compare_op!(self, rhs, gte, "ge")
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
impl SeriesBinaryOps for ArrayWrapper<Int128Array> {}
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
impl SeriesBinaryOps for ArrayWrapper<Decimal128Array> {}
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
impl SeriesBinaryOps for ArrayWrapper<TensorArray> {}
impl SeriesBinaryOps for ArrayWrapper<FixedShapeTensorArray> {}
