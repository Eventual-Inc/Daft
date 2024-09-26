use std::ops::{Add, Div, Mul, Rem, Sub};

use common_error::DaftResult;
use daft_schema::dtype::DataType;

#[cfg(feature = "python")]
use crate::series::utils::python_fn::run_python_binary_operator_fn;
use crate::{
    datatypes::{InferDataType, Utf8Array},
    prelude::Float64Array,
    series::{utils::cast::cast_downcast_op, IntoSeries, Series},
    with_match_numeric_daft_types,
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

impl Add for &Series {
    type Output = DaftResult<Series>;
    fn add(self, rhs: Self) -> Self::Output {
        let output_type =
            InferDataType::from(self.data_type()).add(InferDataType::from(rhs.data_type()))?;
        let lhs = self;
        match &output_type {
            #[cfg(feature = "python")]
            DataType::Python => run_python_binary_operator_fn(lhs, rhs, "add"),
            DataType::Utf8 => {
                Ok(cast_downcast_op!(lhs, rhs, &DataType::Utf8, Utf8Array, add)?.into_series())
            }
            output_type if output_type.is_numeric() => {
                with_match_numeric_daft_types!(output_type, |$T| {
                    Ok(cast_downcast_op!(lhs, rhs, output_type, <$T as DaftDataType>::ArrayType, add)?.into_series())
                })
            }
            output_type if output_type.is_fixed_size_numeric() => {
                todo!("fixed_sized_numeric_binary_op +")
                // fixed_sized_numeric_binary_op!(&lhs, rhs, output_type, add)
            }
            _ => todo!("+"),
            // _ => binary_op_unimplemented!(lhs, "+", rhs, output_type),
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
            #[cfg(feature = "python")]
            DataType::Python => run_python_binary_operator_fn(lhs, rhs, "sub"),
            output_type if output_type.is_numeric() => {
                with_match_numeric_daft_types!(output_type, |$T| {
                    Ok(cast_downcast_op!(lhs, rhs, output_type, <$T as DaftDataType>::ArrayType, sub)?.into_series())
                })
            }
            output_type
                if output_type.is_temporal() || matches!(output_type, DataType::Duration(..)) =>
            {
                match (self.data_type(), rhs.data_type()) {
                    (DataType::Date, DataType::Duration(tu2)) => {
                        let days = rhs.duration()?.cast_to_days()?;
                        let physical_result = self.date()?.physical.sub(&days)?;
                        physical_result.cast(output_type)
                    }
                    (DataType::Date, DataType::Date) => {
                        let physical_result = self.date()?.physical.sub(&rhs.date()?.physical)?;
                        physical_result.cast(output_type)
                    }
                    (DataType::Duration(tu1), DataType::Duration(tu2)) => {
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
                    _ => binary_op_unimplemented!(self, "-", rhs, output_type),
                }
            }
            output_type if output_type.is_fixed_size_numeric() => {
                todo!("fixed_sized_numeric_binary_op -")
                // fixed_sized_numeric_binary_op!(&lhs, $rhs, output_type, $op)
            }
            _ => todo!("Output Type {output_type}"),
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
            #[cfg(feature = "python")]
            DataType::Python => run_python_binary_operator_fn(lhs, rhs, "mul"),
            output_type if output_type.is_numeric() => {
                with_match_numeric_daft_types!(output_type, |$T| {
                    Ok(cast_downcast_op!(lhs, rhs, output_type, <$T as DaftDataType>::ArrayType, mul)?.into_series())
                })
            }
            output_type if output_type.is_fixed_size_numeric() => {
                todo!("fixed_sized_numeric_binary_op *")
                // fixed_sized_numeric_binary_op!(&lhs, $rhs, output_type, $op)
            }
            _ => todo!("Output Type {output_type}"),
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
            #[cfg(feature = "python")]
            DataType::Python => run_python_binary_operator_fn(lhs, rhs, "truediv"),
            DataType::Float64 => {
                Ok(
                    cast_downcast_op!(lhs, rhs, &DataType::Float64, Float64Array, div)?
                        .into_series(),
                )
            }
            output_type if output_type.is_fixed_size_numeric() => {
                todo!("fixed_sized_numeric_binary_op //")
                // fixed_sized_numeric_binary_op!(&lhs, $rhs, output_type, $op)
            }
            _ => todo!("Output Type {output_type}"),
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
                todo!("fixed_sized_numeric_binary_op %")
                // fixed_sized_numeric_binary_op!(&lhs, $rhs, output_type, $op)
            }
            _ => todo!("Output Type {output_type}"),
        }
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
        datatypes::{DataType, Float64Array, Int64Array, Utf8Array},
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
