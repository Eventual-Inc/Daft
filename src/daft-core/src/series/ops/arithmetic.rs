use std::ops::{Add, Div, Mul, Rem, Sub};

use common_error::DaftResult;
use daft_schema::prelude::*;

#[cfg(feature = "python")]
use crate::series::utils::python_fn::run_python_binary_operator_fn;
use crate::{
    array::prelude::*,
    datatypes::{InferDataType, Utf8Array},
    series::{utils::cast::cast_downcast_op, IntoSeries, Series},
    with_match_integer_daft_types, with_match_numeric_daft_types,
};

macro_rules! impl_arithmetic_ref_for_series {
    ($trait:ident, $op:ident) => {
        impl $trait for Series {
            type Output = DaftResult<Series>;
            fn $op(self, rhs: Self) -> Self::Output {
                (&self).$op(&rhs)
            }
        }
    };
}

macro_rules! arithmetic_op_not_implemented {
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

impl Add for &Series {
    type Output = DaftResult<Series>;
    fn add(self, rhs: Self) -> Self::Output {
        let output_type =
            InferDataType::from(self.data_type()).add(InferDataType::from(rhs.data_type()))?;
        let lhs = self;
        match &output_type {
            // ----------------
            // Python
            // ----------------
            #[cfg(feature = "python")]
            DataType::Python => run_python_binary_operator_fn(lhs, rhs, "add"),
            // ----------------
            // Utf8
            // ----------------
            DataType::Utf8 => {
                Ok(cast_downcast_op!(lhs, rhs, &DataType::Utf8, Utf8Array, add)?.into_series())
            }
            // ----------------
            // Numeric types
            // ----------------
            output_type if output_type.is_numeric() => {
                with_match_numeric_daft_types!(output_type, |$T| {
                    Ok(cast_downcast_op!(lhs, rhs, output_type, <$T as DaftDataType>::ArrayType, add)?.into_series())
                })
            }
            // ----------------
            // Decimal Types
            // ----------------
            DataType::Decimal128(..) => {
                Ok(cast_downcast_op!(lhs, rhs, &output_type, Decimal128Array, add)?.into_series())
            }
            // ----------------
            // FixedSizeLists of numeric types (fsl, embedding, tensor, etc.)
            // ----------------
            output_type if output_type.is_fixed_size_numeric() => {
                fixed_size_binary_op(lhs, rhs, output_type, FixedSizeBinaryOp::Add)
            }
            // ----------------
            // Temporal types
            // ----------------
            output_type
                if output_type.is_temporal()
                    || matches!(output_type, DataType::Duration(..) | DataType::Interval) =>
            {
                match (self.data_type(), rhs.data_type()) {
                    // ----------------
                    // Duration
                    // ----------------
                    (DataType::Date, DataType::Duration(..)) => {
                        let days = rhs.duration()?.cast_to_days()?;
                        let physical_result = self.date()?.physical.add(&days)?;
                        physical_result.cast(output_type)
                    }
                    (DataType::Duration(..), DataType::Date) => {
                        let days = lhs.duration()?.cast_to_days()?;
                        let physical_result = days.add(&rhs.date()?.physical)?;
                        physical_result.cast(output_type)
                    }
                    (DataType::Duration(..), DataType::Duration(..)) => {
                        let physical_result =
                            lhs.duration()?.physical.add(&rhs.duration()?.physical)?;
                        physical_result.cast(output_type)
                    }
                    (DataType::Timestamp(..), DataType::Duration(..)) => {
                        let physical_result =
                            self.timestamp()?.physical.add(&rhs.duration()?.physical)?;
                        physical_result.cast(output_type)
                    }
                    (DataType::Duration(..), DataType::Timestamp(..)) => {
                        let physical_result =
                            lhs.duration()?.physical.add(&rhs.timestamp()?.physical)?;
                        physical_result.cast(output_type)
                    }
                    // ----------------
                    // Interval
                    // ----------------
                    (DataType::Timestamp(..), DataType::Interval) => {
                        let ts = self.timestamp()?.add_interval(rhs.interval()?)?;
                        ts.cast(output_type)
                    }
                    (DataType::Interval, DataType::Timestamp(..)) => {
                        let ts = rhs.timestamp()?.add_interval(self.interval()?)?;
                        ts.cast(output_type)
                    }
                    (DataType::Date, DataType::Interval) => {
                        let ts = self.cast(&DataType::Timestamp(TimeUnit::Milliseconds, None))?;
                        let ts = ts.timestamp()?.add_interval(rhs.interval()?)?;
                        ts.cast(output_type)
                    }
                    (DataType::Interval, DataType::Date) => {
                        let ts = rhs.cast(&DataType::Timestamp(TimeUnit::Milliseconds, None))?;
                        let ts = ts.timestamp()?.add_interval(self.interval()?)?;
                        ts.cast(output_type)
                    }

                    _ => arithmetic_op_not_implemented!(self, "+", rhs, output_type),
                }
            }
            _ => arithmetic_op_not_implemented!(self, "+", rhs, output_type),
        }
    }
}

impl Sub for &Series {
    type Output = DaftResult<Series>;
    fn sub(self, rhs: Self) -> Self::Output {
        let output_type =
            InferDataType::from(self.data_type()).sub(InferDataType::from(rhs.data_type()))?;
        let lhs = self;
        match &output_type {
            // ----------------
            // Python
            // ----------------
            #[cfg(feature = "python")]
            DataType::Python => run_python_binary_operator_fn(lhs, rhs, "sub"),
            // ----------------
            // Numeric types
            // ----------------
            output_type if output_type.is_numeric() => {
                with_match_numeric_daft_types!(output_type, |$T| {
                    Ok(cast_downcast_op!(lhs, rhs, output_type, <$T as DaftDataType>::ArrayType, sub)?.into_series())
                })
            }
            // ----------------
            // FixedSizeLists of numeric types (fsl, embedding, tensor, etc.)
            // ----------------
            output_type if output_type.is_fixed_size_numeric() => {
                fixed_size_binary_op(lhs, rhs, output_type, FixedSizeBinaryOp::Sub)
            }
            // ----------------
            // Decimal Types
            // ----------------
            DataType::Decimal128(..) => {
                Ok(cast_downcast_op!(lhs, rhs, &output_type, Decimal128Array, sub)?.into_series())
            }
            // ----------------
            // Temporal types
            // ----------------
            output_type
                if output_type.is_temporal()
                    || matches!(output_type, DataType::Duration(..) | DataType::Interval) =>
            {
                match (self.data_type(), rhs.data_type()) {
                    // ----------------
                    // Duration
                    // ----------------
                    (DataType::Date, DataType::Duration(..)) => {
                        let days = rhs.duration()?.cast_to_days()?;
                        let physical_result = self.date()?.physical.sub(&days)?;
                        physical_result.cast(output_type)
                    }
                    (DataType::Date, DataType::Date) => {
                        let physical_result = self.date()?.physical.sub(&rhs.date()?.physical)?;
                        physical_result.cast(output_type)
                    }
                    (DataType::Duration(..), DataType::Duration(..)) => {
                        let physical_result =
                            lhs.duration()?.physical.sub(&rhs.duration()?.physical)?;
                        physical_result.cast(output_type)
                    }
                    (DataType::Timestamp(..), DataType::Duration(..)) => {
                        let physical_result =
                            self.timestamp()?.physical.sub(&rhs.duration()?.physical)?;
                        physical_result.cast(output_type)
                    }
                    (DataType::Timestamp(..), DataType::Timestamp(..)) => {
                        let physical_result =
                            self.timestamp()?.physical.sub(&rhs.timestamp()?.physical)?;
                        physical_result.cast(output_type)
                    }
                    // ----------------
                    // Interval
                    // ----------------
                    (DataType::Timestamp(..), DataType::Interval) => {
                        let ts = self.timestamp()?.sub_interval(rhs.interval()?)?;
                        ts.cast(output_type)
                    }
                    (DataType::Interval, DataType::Timestamp(..)) => {
                        let ts = rhs.timestamp()?.sub_interval(self.interval()?)?;
                        ts.cast(output_type)
                    }
                    (DataType::Date, DataType::Interval) => {
                        let ts = self.cast(&DataType::Timestamp(TimeUnit::Milliseconds, None))?;
                        let ts = ts.timestamp()?.sub_interval(rhs.interval()?)?;
                        ts.cast(output_type)
                    }
                    (DataType::Interval, DataType::Date) => {
                        let ts = rhs.cast(&DataType::Timestamp(TimeUnit::Milliseconds, None))?;
                        let ts = ts.timestamp()?.sub_interval(self.interval()?)?;
                        ts.cast(output_type)
                    }
                    _ => arithmetic_op_not_implemented!(self, "-", rhs, output_type),
                }
            }

            _ => arithmetic_op_not_implemented!(self, "-", rhs, output_type),
        }
    }
}

impl Mul for &Series {
    type Output = DaftResult<Series>;
    fn mul(self, rhs: Self) -> Self::Output {
        let output_type =
            InferDataType::from(self.data_type()).mul(InferDataType::from(rhs.data_type()))?;
        let lhs = self;
        match &output_type {
            // ----------------
            // Python
            // ----------------
            #[cfg(feature = "python")]
            DataType::Python => run_python_binary_operator_fn(lhs, rhs, "mul"),
            // ----------------
            // Numeric types
            // ----------------
            output_type if output_type.is_numeric() => {
                with_match_numeric_daft_types!(output_type, |$T| {
                    Ok(cast_downcast_op!(lhs, rhs, output_type, <$T as DaftDataType>::ArrayType, mul)?.into_series())
                })
            }
            // ----------------
            // Decimal Types
            // ----------------
            DataType::Decimal128(..) => {
                Ok(cast_downcast_op!(lhs, rhs, &output_type, Decimal128Array, mul)?.into_series())
            }
            // ----------------
            // FixedSizeLists of numeric types (fsl, embedding, tensor, etc.)
            // ----------------
            output_type if output_type.is_fixed_size_numeric() => {
                fixed_size_binary_op(lhs, rhs, output_type, FixedSizeBinaryOp::Mul)
            }
            _ => arithmetic_op_not_implemented!(self, "*", rhs, output_type),
        }
    }
}

impl Div for &Series {
    type Output = DaftResult<Series>;
    fn div(self, rhs: Self) -> Self::Output {
        let output_type =
            InferDataType::from(self.data_type()).div(InferDataType::from(rhs.data_type()))?;
        let lhs = self;
        match &output_type {
            // ----------------
            // Python
            // ----------------
            #[cfg(feature = "python")]
            DataType::Python => run_python_binary_operator_fn(lhs, rhs, "truediv"),
            // ----------------
            // Numeric types
            // ----------------
            DataType::Float64 => {
                Ok(
                    cast_downcast_op!(lhs, rhs, &DataType::Float64, Float64Array, div)?
                        .into_series(),
                )
            }
            // ----------------
            // Decimal Types
            // ----------------
            DataType::Decimal128(..) => {
                Ok(cast_downcast_op!(lhs, rhs, &output_type, Decimal128Array, div)?.into_series())
            }
            // ----------------
            // FixedSizeLists of numeric types (fsl, embedding, tensor, etc.)
            // ----------------
            output_type if output_type.is_fixed_size_numeric() => {
                fixed_size_binary_op(lhs, rhs, output_type, FixedSizeBinaryOp::Div)
            }
            _ => arithmetic_op_not_implemented!(self, "/", rhs, output_type),
        }
    }
}

impl Rem for &Series {
    type Output = DaftResult<Series>;
    fn rem(self, rhs: Self) -> Self::Output {
        let output_type =
            InferDataType::from(self.data_type()).rem(InferDataType::from(rhs.data_type()))?;

        let lhs = self;
        match &output_type {
            #[cfg(feature = "python")]
            DataType::Python => run_python_binary_operator_fn(lhs, rhs, "mod"),
            output_type if output_type.is_numeric() => {
                with_match_numeric_daft_types!(output_type, |$T| {
                    Ok(cast_downcast_op!(lhs, rhs, output_type, <$T as DaftDataType>::ArrayType, rem)?.into_series())
                })
            }
            output_type if output_type.is_fixed_size_numeric() => {
                fixed_size_binary_op(lhs, rhs, output_type, FixedSizeBinaryOp::Rem)
            }
            _ => arithmetic_op_not_implemented!(self, "%", rhs, output_type),
        }
    }
}

impl Series {
    pub fn floor_div(&self, rhs: &Self) -> DaftResult<Self> {
        let output_type = InferDataType::from(self.data_type())
            .floor_div(&InferDataType::from(rhs.data_type()))?;
        let lhs = self;
        match &output_type {
            #[cfg(feature = "python")]
            DataType::Python => run_python_binary_operator_fn(lhs, rhs, "floordiv"),
            output_type if output_type.is_integer() => {
                with_match_integer_daft_types!(output_type, |$T| {
                    Ok(cast_downcast_op!(lhs, rhs, output_type, <$T as DaftDataType>::ArrayType, div)?.into_series())
                })
            }
            output_type if output_type.is_numeric() => {
                let div_floor = lhs.div(rhs)?.floor()?;
                div_floor.cast(output_type)
            }
            _ => arithmetic_op_not_implemented!(self, "floor_div", rhs, output_type),
        }
    }
}

enum FixedSizeBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

fn fixed_size_binary_op(
    left: &Series,
    right: &Series,
    output_type: &DataType,
    op: FixedSizeBinaryOp,
) -> DaftResult<Series> {
    fn run_fixed_size_binary_op<A>(lhs: &A, rhs: &A, op: FixedSizeBinaryOp) -> DaftResult<A>
    where
        for<'a> &'a A: Add<Output = DaftResult<A>>
            + Sub<Output = DaftResult<A>>
            + Mul<Output = DaftResult<A>>
            + Div<Output = DaftResult<A>>
            + Rem<Output = DaftResult<A>>,
    {
        match op {
            FixedSizeBinaryOp::Add => lhs.add(rhs),
            FixedSizeBinaryOp::Sub => lhs.sub(rhs),
            FixedSizeBinaryOp::Mul => lhs.mul(rhs),
            FixedSizeBinaryOp::Div => lhs.div(rhs),
            FixedSizeBinaryOp::Rem => lhs.rem(rhs),
        }
    }

    match (left.data_type(), right.data_type()) {
        (DataType::FixedSizeList(..), DataType::FixedSizeList(..)) => {
            let array = run_fixed_size_binary_op(
                left.downcast::<FixedSizeListArray>().unwrap(),
                right.downcast::<FixedSizeListArray>().unwrap(),
                op,
            )?;
            Ok(array.into_series())
        }
        (DataType::Embedding(..), DataType::Embedding(..)) => {
            let physical = run_fixed_size_binary_op(
                &left.downcast::<EmbeddingArray>().unwrap().physical,
                &right.downcast::<EmbeddingArray>().unwrap().physical,
                op,
            )?;
            let array = EmbeddingArray::new(Field::new(left.name(), output_type.clone()), physical);
            Ok(array.into_series())
        }
        (DataType::FixedShapeTensor(..), DataType::FixedShapeTensor(..)) => {
            let physical = run_fixed_size_binary_op(
                &left.downcast::<FixedShapeTensorArray>().unwrap().physical,
                &right.downcast::<FixedShapeTensorArray>().unwrap().physical,
                op,
            )?;
            let array =
                FixedShapeTensorArray::new(Field::new(left.name(), output_type.clone()), physical);
            Ok(array.into_series())
        }
        (left, right) => unimplemented!("cannot add {left} and {right} types"),
    }
}

impl_arithmetic_ref_for_series!(Add, add);
impl_arithmetic_ref_for_series!(Sub, sub);
impl_arithmetic_ref_for_series!(Mul, mul);
impl_arithmetic_ref_for_series!(Div, div);
impl_arithmetic_ref_for_series!(Rem, rem);

#[cfg(test)]
mod tests {
    use common_error::DaftResult;

    use crate::{
        array::ops::full::FullNull,
        datatypes::{DataType, Float32Array, Float64Array, Int32Array, Int64Array, Utf8Array},
        series::IntoSeries,
    };

    #[test]
    fn add_int_and_int() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3]));
        let b = Int64Array::from(("b", vec![1, 2, 3]));
        let c = a.into_series() + b.into_series();
        assert_eq!(*c?.data_type(), DataType::Int64);
        Ok(())
    }

    #[test]
    fn add_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3]));
        let b = Float64Array::from(("b", vec![1., 2., 3.]));
        let c = a.into_series() + b.into_series();
        assert_eq!(*c?.data_type(), DataType::Float64);
        Ok(())
    }
    #[test]
    fn sub_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3]));
        let b = Float64Array::from(("b", vec![1., 2., 3.]));
        let c = a.into_series() - b.into_series();
        assert_eq!(*c?.data_type(), DataType::Float64);
        Ok(())
    }

    #[test]
    fn mul_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3]));
        let b = Float64Array::from(("b", vec![1., 2., 3.]));
        let c = a.into_series() * b.into_series();
        assert_eq!(*c?.data_type(), DataType::Float64);
        Ok(())
    }
    #[test]
    fn div_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3]));
        let b = Float64Array::from(("b", vec![1., 2., 3.]));
        let c = a.into_series() / b.into_series();
        assert_eq!(*c?.data_type(), DataType::Float64);
        Ok(())
    }
    #[test]
    fn floor_div_int_and_int() -> DaftResult<()> {
        let a = Int32Array::from(("a", vec![1, 2, 3]));
        let b = Int64Array::from(("b", vec![1, 2, 3]));
        let c = a.into_series().floor_div(&(b.into_series()));
        assert_eq!(*c?.data_type(), DataType::Int64);
        Ok(())
    }
    #[test]
    fn floor_div_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3]));
        let b = Float64Array::from(("b", vec![1., 2., 3.]));
        let c = a.into_series().floor_div(&(b.into_series()));
        assert_eq!(*c?.data_type(), DataType::Float64);
        Ok(())
    }
    #[test]
    fn floor_div_float_and_float() -> DaftResult<()> {
        let a = Float32Array::from(("b", vec![1., 2., 3.]));
        let b = Float64Array::from(("b", vec![1., 2., 3.]));
        let c = a.into_series().floor_div(&(b.into_series()));
        assert_eq!(*c?.data_type(), DataType::Float64);
        Ok(())
    }
    #[test]
    fn rem_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3]));
        let b = Float64Array::from(("b", vec![1., 2., 3.]));
        let c = a.into_series() % b.into_series();
        assert_eq!(*c?.data_type(), DataType::Float64);
        Ok(())
    }
    #[test]
    fn add_int_and_int_full_null() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3]));
        let b = Int64Array::full_null("b", &DataType::Int64, 3);
        let c = a.into_series() + b.into_series();
        assert_eq!(*c?.data_type(), DataType::Int64);
        Ok(())
    }
    #[test]
    fn add_int_and_utf8() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3]));
        let str_array = vec!["a", "b", "c"];
        let b = Utf8Array::from(("b", str_array.as_slice()));
        let c = a.into_series() + b.into_series();
        assert_eq!(*c?.data_type(), DataType::Utf8);
        Ok(())
    }
}
