use std::ops::{Add, Div, Mul, Rem, Sub};

use super::match_types_on_series;
use super::py_binary_op_utilfn;

use crate::datatypes::{DataType, Float64Type};
use crate::error::{DaftError, DaftResult};
use crate::series::{IntoSeries, Series};
use crate::with_match_numeric_and_utf_daft_types;
use crate::with_match_numeric_daft_types;

macro_rules! py_binary_op {
    ($lhs:expr, $rhs:expr, $pyoperator:expr) => {
        py_binary_op_utilfn!($lhs, $rhs, $pyoperator, "map_operator_arrow_semantics")
    };
}

macro_rules! impl_series_math_op {
    ($op:ident, $func_name:ident, $pyop:expr) => {
        impl $op for &Series {
            type Output = DaftResult<Series>;
            fn $func_name(self, rhs: Self) -> Self::Output {
                let (lhs, rhs) = match_types_on_series(self, rhs)?;

                #[cfg(feature = "python")]
                if lhs.data_type() == &DataType::Python {
                    return Ok(py_binary_op!(lhs, rhs, $pyop));
                }

                if !lhs.data_type().is_numeric() || !rhs.data_type().is_numeric() {
                    return Err(DaftError::TypeError(
                        "Cannot run on non-numeric types".into(),
                    ));
                }
                with_match_numeric_daft_types!(lhs.data_type(), |$T| {
                    let lhs = lhs.downcast::<$T>()?;
                    let rhs = rhs.downcast::<$T>()?;
                    Ok(lhs.$func_name(rhs)?.into_series().rename(lhs.name()))
                })
            }
        }

        impl $op for Series {
            type Output = DaftResult<Series>;
            fn $func_name(self, rhs: Self) -> Self::Output {
                (&self).$func_name(&rhs)
            }
        }
    };
}

impl Add for &Series {
    type Output = DaftResult<Series>;
    fn add(self, rhs: Self) -> Self::Output {
        let (lhs, rhs) = match_types_on_series(self, rhs)?;

        #[cfg(feature = "python")]
        if lhs.data_type() == &DataType::Python {
            return Ok(py_binary_op!(lhs, rhs, "add"));
        }

        with_match_numeric_and_utf_daft_types!(lhs.data_type(), |$T| {
            let lhs = lhs.downcast::<$T>()?;
            let rhs = rhs.downcast::<$T>()?;
            Ok(lhs.add(rhs)?.into_series().rename(lhs.name()))
        })
    }
}

impl Add for Series {
    type Output = DaftResult<Series>;
    fn add(self, rhs: Self) -> Self::Output {
        (&self).add(&rhs)
    }
}

impl Div for &Series {
    type Output = DaftResult<Series>;
    fn div(self, rhs: Self) -> Self::Output {
        let (lhs, rhs) = match_types_on_series(self, rhs)?;

        #[cfg(feature = "python")]
        if lhs.data_type() == &DataType::Python {
            return Ok(py_binary_op!(lhs, rhs, "truediv"));
        }

        if !self.data_type().is_numeric() || !rhs.data_type().is_numeric() {
            return Err(DaftError::TypeError(format!(
                "True division requires numeric arguments, but received {} / {}",
                self.data_type(),
                rhs.data_type()
            )));
        }
        let lhs = self.cast(&crate::datatypes::DataType::Float64)?;
        let rhs = rhs.cast(&crate::datatypes::DataType::Float64)?;

        Ok(lhs
            .downcast::<Float64Type>()?
            .div(rhs.downcast::<Float64Type>()?)?
            .into_series()
            .rename(lhs.name()))
    }
}

impl Div for Series {
    type Output = DaftResult<Series>;
    fn div(self, rhs: Self) -> Self::Output {
        (&self).div(&rhs)
    }
}

impl_series_math_op!(Sub, sub, "sub");
impl_series_math_op!(Mul, mul, "mul");
impl_series_math_op!(Rem, rem, "mod");

#[cfg(test)]
mod tests {
    use crate::series::IntoSeries;
    use crate::{
        datatypes::{DataType, Float64Array, Int64Array, Utf8Array},
        error::DaftResult,
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
