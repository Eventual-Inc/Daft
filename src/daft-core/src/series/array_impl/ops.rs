use std::ops::{Add, Div, Mul, Rem, Sub};

use common_error::{DaftError, DaftResult};

use crate::{
    series::{ops::py_binary_op_utilfn, series_like::SeriesLike},
    with_match_numeric_and_utf_daft_types, DataType,
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
use crate::datatypes::PythonArray;

macro_rules! py_binary_op {
    ($lhs:expr, $rhs:expr, $pyoperator:expr) => {
        py_binary_op_utilfn!($lhs, $rhs, $pyoperator, "map_operator_arrow_semantics")
    };
}

fn default_add(lhs: &Series, rhs: &Series, output_type: &DataType) -> DaftResult<Series> {
    use DataType::*;
    match output_type {
        #[cfg(feature = "python")]
        Python => Ok(py_binary_op!(lhs, rhs, "add")),
        output_type if output_type.is_physical() => {
            let lhs = lhs.cast(&output_type)?;
            let rhs = rhs.cast(&output_type)?;
            with_match_numeric_and_utf_daft_types!(output_type, |$T| {
                let lhs = lhs.downcast::<$T>()?;
                let rhs = rhs.downcast::<$T>()?;
                Ok(lhs.add(rhs)?.into_series().rename(lhs.name()))
            })
        }
        _ => panic!(
            "No default add implementation for {} + {} -> {}",
            lhs.data_type(),
            rhs.data_type(),
            output_type
        ),
    }
}

impl Add<&Series> for &ArrayWrapper<TimestampArray> {
    type Output = DaftResult<Series>;

    fn add(self, rhs: &Series) -> DaftResult<Series> {
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
            _ => default_add(&lhs, rhs, &output_type),
        }
    }
}

impl Add<&Series> for &ArrayWrapper<DurationArray> {
    type Output = DaftResult<Series>;

    fn add(self, rhs: &Series) -> DaftResult<Series> {
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
            _ => default_add(&lhs, rhs, &output_type),
        }
    }
}

macro_rules! impl_default_add {
    ($arrayT:ty) => {
        impl Add<&Series> for &ArrayWrapper<$arrayT> {
            type Output = DaftResult<Series>;

            fn add(self, rhs: &Series) -> DaftResult<Series> {
                let output_type = (self.data_type() + rhs.data_type())?;
                let lhs = self.0.clone().into_series();
                default_add(&lhs, rhs, &output_type)
            }
        }
    };
}

#[cfg(feature = "python")]
impl_default_add!(PythonArray);
impl_default_add!(NullArray);
impl_default_add!(BooleanArray);
impl_default_add!(BinaryArray);
impl_default_add!(Int8Array);
impl_default_add!(Int16Array);
impl_default_add!(Int32Array);
impl_default_add!(Int64Array);
impl_default_add!(UInt8Array);
impl_default_add!(UInt16Array);
impl_default_add!(UInt32Array);
impl_default_add!(UInt64Array);
impl_default_add!(Float32Array);
impl_default_add!(Float64Array);
impl_default_add!(Utf8Array);
impl_default_add!(FixedSizeListArray);
impl_default_add!(ListArray);
impl_default_add!(StructArray);
impl_default_add!(ExtensionArray);

impl_default_add!(DateArray);
impl_default_add!(EmbeddingArray);
impl_default_add!(ImageArray);
impl_default_add!(FixedShapeImageArray);
