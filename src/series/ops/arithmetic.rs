use std::ops::{Add, Div, Mul, Rem, Sub};

use crate::{
    array::BaseArray, error::DaftResult, series::Series, utils::supertype::try_get_supertype,
};

fn match_types_on_series(l: &Series, r: &Series) -> DaftResult<(Series, Series)> {
    let mut lhs = l.clone();
    let mut rhs = r.clone();
    let supertype = try_get_supertype(lhs.data_type(), rhs.data_type())?;
    if !lhs.data_type().eq(&supertype) {
        lhs = lhs.cast(&supertype)?;
    }

    if !rhs.data_type().eq(&supertype) {
        rhs = rhs.cast(&supertype)?;
    }
    Ok((lhs, rhs))
}

#[macro_export]
macro_rules! with_match_numeric_daft_types {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::DataType::*;
    use $crate::datatypes::*;

    match $key_type {
        Int8 => __with_ty__! { Int8Type },
        Int16 => __with_ty__! { Int16Type },
        Int32 => __with_ty__! { Int32Type },
        Int64 => __with_ty__! { Int64Type },
        UInt8 => __with_ty__! { UInt8Type },
        UInt16 => __with_ty__! { UInt16Type },
        UInt32 => __with_ty__! { UInt32Type },
        UInt64 => __with_ty__! { UInt64Type },
        // Float16 => __with_ty__! { Float16Type },
        Float32 => __with_ty__! { Float32Type },
        Float64 => __with_ty__! { Float64Type },
        _ => panic!("{:?} not implemented", $key_type)
    }
})}

#[macro_export]
macro_rules! with_match_numeric_and_utf_daft_types {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::DataType::*;
    use $crate::datatypes::*;

    match $key_type {
        Int8 => __with_ty__! { Int8Type },
        Int16 => __with_ty__! { Int16Type },
        Int32 => __with_ty__! { Int32Type },
        Int64 => __with_ty__! { Int64Type },
        UInt8 => __with_ty__! { UInt8Type },
        UInt16 => __with_ty__! { UInt16Type },
        UInt32 => __with_ty__! { UInt32Type },
        UInt64 => __with_ty__! { UInt64Type },
        // Float16 => __with_ty__! { Float16Type },
        Float32 => __with_ty__! { Float32Type },
        Float64 => __with_ty__! { Float64Type },
        Utf8 => __with_ty__! { Utf8Type },
        _ => panic!("{:?} not implemented", $key_type)
    }
})}

macro_rules! impl_series_math_op {
    ($op:ident, $func_name:ident) => {
        impl $op for &Series {
            type Output = Series;
            fn $func_name(self, rhs: Self) -> Self::Output {
                let (lhs, rhs) = match_types_on_series(self, rhs).unwrap();
                with_match_numeric_daft_types!(lhs.data_type(), |$T| {
                    let lhs = lhs.downcast::<$T>().unwrap();
                    let rhs = rhs.downcast::<$T>().unwrap();
                    lhs.$func_name(rhs).into_series()
                })
            }
        }

        impl $op for Series {
            type Output = Series;
            fn $func_name(self, rhs: Self) -> Self::Output {
                (&self).$func_name(&rhs)
            }
        }
    };
}

impl Add for &Series {
    type Output = Series;
    fn add(self, rhs: Self) -> Self::Output {
        let (lhs, rhs) = match_types_on_series(self, rhs).unwrap();
        with_match_numeric_and_utf_daft_types!(lhs.data_type(), |$T| {
            let lhs = lhs.downcast::<$T>().unwrap();
            let rhs = rhs.downcast::<$T>().unwrap();
            lhs.add(rhs).into_series()
        })
    }
}

impl Add for Series {
    type Output = Series;
    fn add(self, rhs: Self) -> Self::Output {
        (&self).add(&rhs)
    }
}

impl_series_math_op!(Sub, sub);
impl_series_math_op!(Div, div);
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
        let a = Int64Array::from(("a", vec![1, 2, 3].as_slice()));
        let b = Int64Array::from(("b", vec![1, 2, 3].as_slice()));
        let c = a.into_series() + b.into_series();
        assert_eq!(*c.data_type(), DataType::Int64);
        Ok(())
    }

    #[test]
    fn add_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3].as_slice()));
        let b = Float64Array::from(("b", vec![1., 2., 3.].as_slice()));
        let c = a.into_series() + b.into_series();
        assert_eq!(*c.data_type(), DataType::Float64);
        Ok(())
    }
    #[test]
    fn sub_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3].as_slice()));
        let b = Float64Array::from(("b", vec![1., 2., 3.].as_slice()));
        let c = a.into_series() - b.into_series();
        assert_eq!(*c.data_type(), DataType::Float64);
        Ok(())
    }

    #[test]
    fn mul_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3].as_slice()));
        let b = Float64Array::from(("b", vec![1., 2., 3.].as_slice()));
        let c = a.into_series() * b.into_series();
        assert_eq!(*c.data_type(), DataType::Float64);
        Ok(())
    }
    #[test]
    fn div_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3].as_slice()));
        let b = Float64Array::from(("b", vec![1., 2., 3.].as_slice()));
        let c = a.into_series() / b.into_series();
        assert_eq!(*c.data_type(), DataType::Float64);
        Ok(())
    }
    #[test]
    fn rem_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3].as_slice()));
        let b = Float64Array::from(("b", vec![1., 2., 3.].as_slice()));
        let c = a.into_series() % b.into_series();
        assert_eq!(*c.data_type(), DataType::Float64);
        Ok(())
    }
    #[test]
    fn add_int_and_int_full_null() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3].as_slice()));
        let b = Int64Array::full_null("b", 3);
        let c = a.into_series() + b.into_series();
        assert_eq!(*c.data_type(), DataType::Int64);
        Ok(())
    }
    #[test]
    fn add_int_and_utf8() -> DaftResult<()> {
        let a = Int64Array::from(("a", vec![1, 2, 3].as_slice()));
        let str_array = vec!["a", "b", "c"];
        let b = Utf8Array::from(("b", str_array.as_slice()));
        let c = a.into_series() + b.into_series();
        assert_eq!(*c.data_type(), DataType::Utf8);
        Ok(())
    }
}
