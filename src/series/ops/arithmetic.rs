use std::ops::{Add, Div, Mul, Rem, Sub};

use super::match_types_on_series;
use crate::array::BaseArray;
use crate::datatypes::Float64Type;
use crate::error::DaftResult;
use crate::series::Series;
use crate::with_match_numeric_and_utf_daft_types;
use crate::with_match_numeric_daft_types;

macro_rules! impl_series_math_op {
    ($op:ident, $func_name:ident) => {
        impl $op for &Series {
            type Output = DaftResult<Series>;
            fn $func_name(self, rhs: Self) -> Self::Output {
                let (lhs, rhs) = match_types_on_series(self, rhs)?;
                with_match_numeric_daft_types!(lhs.data_type(), |$T| {
                    let lhs = lhs.downcast::<$T>()?;
                    let rhs = rhs.downcast::<$T>()?;
                    Ok(lhs.$func_name(rhs)?.into_series())
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
        with_match_numeric_and_utf_daft_types!(lhs.data_type(), |$T| {
            let lhs = lhs.downcast::<$T>()?;
            let rhs = rhs.downcast::<$T>()?;
            Ok(lhs.add(rhs)?.into_series())
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
        let lhs = self.cast(&crate::datatypes::DataType::Float64)?;
        let rhs = rhs.cast(&crate::datatypes::DataType::Float64)?;

        Ok(lhs
            .downcast::<Float64Type>()?
            .div(rhs.downcast::<Float64Type>()?)?
            .into_series())
    }
}

impl Div for Series {
    type Output = DaftResult<Series>;
    fn div(self, rhs: Self) -> Self::Output {
        (&self).div(&rhs)
    }
}

impl_series_math_op!(Sub, sub);
impl_series_math_op!(Mul, mul);
impl_series_math_op!(Rem, rem);

#[cfg(test)]
mod tests {
    use crate::{
        array::BaseArray,
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
        let b = Int64Array::full_null("b", 3);
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
