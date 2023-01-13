use std::ops::{Add, Div, Mul, Rem, Sub};

use crate::{
    array::data_array::BaseArray, error::DaftResult, series::Series,
    utils::supertype::try_get_supertype,
};

// pub fn dispatch_binary_op(lhs: &dyn BaseArray, rhs: &dyn BaseArray, func) -> Series {

// }

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
macro_rules! with_match_arrow_daft_types {(
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

macro_rules! impl_series_math_op {
    ($op:ident, $func_name:ident) => {
        impl $op for &Series {
            type Output = Series;
            fn $func_name(self, rhs: Self) -> Self::Output {
                let (lhs, rhs) = match_types_on_series(self, rhs).unwrap();
                with_match_arrow_daft_types!(lhs.data_type(), |$T| {
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

impl_series_math_op!(Add, add);
impl_series_math_op!(Sub, sub);
impl_series_math_op!(Div, div);
impl_series_math_op!(Mul, mul);
impl_series_math_op!(Rem, rem);

mod tests {

    #[test]
    fn add_int_and_int() -> DaftResult<()> {
        let a = Int64Array::from(vec![1, 2, 3].as_slice());
        let b = Int64Array::from(vec![1, 2, 3].as_slice());
        let c = a.into_series() + b.into_series();
        assert_eq!(*c.data_type(), DataType::Int64);
        Ok(())
    }

    #[test]
    fn add_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(vec![1, 2, 3].as_slice());
        let b = Float64Array::from(vec![1., 2., 3.].as_slice());
        let c = a.into_series() + b.into_series();
        assert_eq!(*c.data_type(), DataType::Float64);
        Ok(())
    }
    #[test]
    fn sub_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(vec![1, 2, 3].as_slice());
        let b = Float64Array::from(vec![1., 2., 3.].as_slice());
        let c = a.into_series() - b.into_series();
        assert_eq!(*c.data_type(), DataType::Float64);
        Ok(())
    }

    #[test]
    fn mul_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(vec![1, 2, 3].as_slice());
        let b = Float64Array::from(vec![1., 2., 3.].as_slice());
        let c = a.into_series() * b.into_series();
        assert_eq!(*c.data_type(), DataType::Float64);
        Ok(())
    }
    #[test]
    fn div_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(vec![1, 2, 3].as_slice());
        let b = Float64Array::from(vec![1., 2., 3.].as_slice());
        let c = a.into_series() / b.into_series();
        assert_eq!(*c.data_type(), DataType::Float64);
        Ok(())
    }
    #[test]
    fn rem_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(vec![1, 2, 3].as_slice());
        let b = Float64Array::from(vec![1., 2., 3.].as_slice());
        let c = a.into_series() % b.into_series();
        assert_eq!(*c.data_type(), DataType::Float64);
        Ok(())
    }
    #[test]
    fn add_int_and_int_full_null() -> DaftResult<()> {
        let a = Int64Array::from(vec![1, 2, 3].as_slice());
        let b = Int64Array::full_null(3);
        let c = a.into_series() + b.into_series();
        assert_eq!(*c.data_type(), DataType::Int64);
        Ok(())
    }
}
