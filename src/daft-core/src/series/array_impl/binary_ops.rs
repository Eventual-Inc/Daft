use std::ops::{Add, Div, Mul, Rem, Sub};

use common_error::DaftResult;

use crate::{
    array::{
        ops::{DaftCompare, DaftLogical},
        FixedSizeListArray, ListArray, StructArray,
    },
    datatypes::{
        logical::{Decimal128Array, MapArray},
        Field, FixedSizeBinaryArray, Int128Array,
    },
    series::series_like::SeriesLike,
    with_match_comparable_daft_types, with_match_integer_daft_types, with_match_numeric_daft_types,
    DataType,
};

use crate::datatypes::logical::{
    DateArray, DurationArray, EmbeddingArray, FixedShapeImageArray, FixedShapeTensorArray,
    ImageArray, TensorArray, TimeArray, TimestampArray,
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

macro_rules! apply_fixed_numeric_op {
    ($lhs:expr, $rhs:expr, $op:ident) => {{
        $lhs.$op($rhs)?
    }};
}

macro_rules! fixed_sized_numeric_binary_op {
    ($left:expr, $right:expr, $output_type:expr, $op:ident) => {{
        assert!($left.data_type().is_fixed_size_numeric());
        assert!($right.data_type().is_fixed_size_numeric());

        match ($left.data_type(), $right.data_type()) {
            (DataType::FixedSizeList(..), DataType::FixedSizeList(..)) => {
                Ok(apply_fixed_numeric_op!(
                    $left.downcast::<FixedSizeListArray>().unwrap(),
                    $right.downcast::<FixedSizeListArray>().unwrap(),
                    $op
                )
                .into_series())
            }
            (DataType::Embedding(..), DataType::Embedding(..)) => {
                let physical = apply_fixed_numeric_op!(
                    &$left.downcast::<EmbeddingArray>().unwrap().physical,
                    &$right.downcast::<EmbeddingArray>().unwrap().physical,
                    $op
                );
                let array =
                    EmbeddingArray::new(Field::new($left.name(), $output_type.clone()), physical);
                Ok(array.into_series())
            }
            (DataType::FixedShapeTensor(..), DataType::FixedShapeTensor(..)) => {
                let physical = apply_fixed_numeric_op!(
                    &$left.downcast::<FixedShapeTensorArray>().unwrap().physical,
                    &$right.downcast::<FixedShapeTensorArray>().unwrap().physical,
                    $op
                );
                let array = FixedShapeTensorArray::new(
                    Field::new($left.name(), $output_type.clone()),
                    physical,
                );
                Ok(array.into_series())
            }
            (left, right) => unimplemented!("cannot add {left} and {right} types"),
        }
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
            output_type if output_type.is_fixed_size_numeric() => {
                fixed_sized_numeric_binary_op!(&lhs, $rhs, output_type, $op)
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
        match &output_type {
            #[cfg(feature = "python")]
            Boolean => match (&lhs.data_type(), &$rhs.data_type()) {
                #[cfg(feature = "python")]
                (Python, _) | (_, Python) => Ok(py_binary_op_bool!(lhs, $rhs, $pyop)),
                _ => cast_downcast_op_into_series!(lhs, $rhs, &Boolean, BooleanArray, $op),
            },
            output_type if output_type.is_integer() => {
                with_match_integer_daft_types!(output_type, |$T| {
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

macro_rules! physical_compare_op {
    ($self:expr, $rhs:expr, $op:ident, $pyop:expr) => {{
        let (output_type, intermediate, comp_type) =
            ($self.data_type().comparison_op($rhs.data_type()))?;
        let lhs = $self.into_series();
        let (lhs, rhs) = if let Some(ref it) = intermediate {
            (lhs.cast(it)?, $rhs.cast(it)?)
        } else {
            (lhs, $rhs.clone())
        };

        use DataType::*;
        if let Boolean = output_type {
            match comp_type {
                #[cfg(feature = "python")]
                Python => py_binary_op_bool!(lhs, rhs, $pyop)
                    .downcast::<BooleanArray>()
                    .cloned(),
                _ => with_match_comparable_daft_types!(comp_type, |$T| {
                    cast_downcast_op!(lhs, rhs, &comp_type, <$T as DaftDataType>::ArrayType, $op)
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
            output_type if output_type.is_fixed_size_numeric() => {
                fixed_sized_numeric_binary_op!(&lhs, rhs, output_type, add)
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
            output_type if output_type.is_fixed_size_numeric() => {
                fixed_sized_numeric_binary_op!(&lhs, rhs, output_type, div)
            }
            _ => binary_op_unimplemented!(lhs, "/", rhs, output_type),
        }
    }
    fn rem(&self, rhs: &Series) -> DaftResult<Series> {
        py_numeric_binary_op!(self, rhs, rem, "mod")
    }
    fn and(&self, rhs: &Series) -> DaftResult<Series> {
        physical_logic_op!(self, rhs, and, "and_")
    }
    fn or(&self, rhs: &Series) -> DaftResult<Series> {
        physical_logic_op!(self, rhs, or, "or_")
    }
    fn xor(&self, rhs: &Series) -> DaftResult<Series> {
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
impl SeriesBinaryOps for ArrayWrapper<FixedSizeBinaryArray> {}
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
impl SeriesBinaryOps for ArrayWrapper<MapArray> {}
impl SeriesBinaryOps for ArrayWrapper<ExtensionArray> {}
impl SeriesBinaryOps for ArrayWrapper<Decimal128Array> {}
impl SeriesBinaryOps for ArrayWrapper<DateArray> {
    fn add(&self, rhs: &Series) -> DaftResult<Series> {
        use DataType::*;
        let output_type = (self.data_type() + rhs.data_type())?;
        match rhs.data_type() {
            Duration(..) => {
                let days = rhs.duration()?.cast_to_days()?;
                let physical_result = self.0.physical.add(&days)?;
                physical_result.cast(&output_type)
            }
            _ => binary_op_unimplemented!(self, "+", rhs, output_type),
        }
    }
    fn sub(&self, rhs: &Series) -> DaftResult<Series> {
        use DataType::*;
        let output_type = (self.data_type() - rhs.data_type())?;
        match rhs.data_type() {
            Date => {
                let physical_result = self.0.physical.sub(&rhs.date()?.physical)?;
                physical_result.cast(&output_type)
            }
            Duration(..) => {
                let days = rhs.duration()?.cast_to_days()?;
                let physical_result = self.0.physical.sub(&days)?;
                physical_result.cast(&output_type)
            }
            _ => binary_op_unimplemented!(self, "-", rhs, output_type),
        }
    }
}
impl SeriesBinaryOps for ArrayWrapper<TimeArray> {}
impl SeriesBinaryOps for ArrayWrapper<DurationArray> {
    fn add(&self, rhs: &Series) -> DaftResult<Series> {
        use DataType::*;
        let output_type = (self.data_type() + rhs.data_type())?;
        let lhs = self.0.clone().into_series();
        match rhs.data_type() {
            Timestamp(..) => {
                let physical_result = self.0.physical.add(&rhs.timestamp()?.physical)?;
                physical_result.cast(&output_type)
            }
            Duration(..) => {
                let physical_result = self.0.physical.add(&rhs.duration()?.physical)?;
                physical_result.cast(&output_type)
            }
            Date => {
                let days = self.0.cast_to_days()?;
                let physical_result = days.add(&rhs.date()?.physical)?;
                physical_result.cast(&output_type)
            }
            _ => binary_op_unimplemented!(lhs, "+", rhs, output_type),
        }
    }

    fn sub(&self, rhs: &Series) -> DaftResult<Series> {
        use DataType::*;
        let output_type = (self.data_type() - rhs.data_type())?;
        match rhs.data_type() {
            Duration(..) => {
                let physical_result = self.0.physical.sub(&rhs.duration()?.physical)?;
                physical_result.cast(&output_type)
            }
            _ => binary_op_unimplemented!(self, "-", rhs, output_type),
        }
    }
}

impl SeriesBinaryOps for ArrayWrapper<TimestampArray> {
    fn add(&self, rhs: &Series) -> DaftResult<Series> {
        use DataType::*;
        let output_type = (self.data_type() + rhs.data_type())?;
        match rhs.data_type() {
            Duration(..) => {
                let physical_result = self.0.physical.add(&rhs.duration()?.physical)?;
                physical_result.cast(&output_type)
            }
            _ => binary_op_unimplemented!(self, "+", rhs, output_type),
        }
    }
    fn sub(&self, rhs: &Series) -> DaftResult<Series> {
        use DataType::*;
        let output_type = (self.data_type() - rhs.data_type())?;
        match rhs.data_type() {
            Duration(..) => {
                let physical_result = self.0.physical.sub(&rhs.duration()?.physical)?;
                physical_result.cast(&output_type)
            }
            Timestamp(..) => {
                let physical_result = self.0.physical.sub(&rhs.timestamp()?.physical)?;
                physical_result.cast(&output_type)
            }
            _ => binary_op_unimplemented!(self, "-", rhs, output_type),
        }
    }
}
impl SeriesBinaryOps for ArrayWrapper<EmbeddingArray> {}
impl SeriesBinaryOps for ArrayWrapper<ImageArray> {}
impl SeriesBinaryOps for ArrayWrapper<FixedShapeImageArray> {}
impl SeriesBinaryOps for ArrayWrapper<TensorArray> {}
impl SeriesBinaryOps for ArrayWrapper<FixedShapeTensorArray> {}
