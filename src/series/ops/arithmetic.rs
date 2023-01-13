use std::ops::Add;

use arrow2::array::MutableArray;

use crate::{array::data_array::BaseArray, series::Series, utils::supertype::get_supertype};

// pub fn dispatch_binary_op(lhs: &dyn BaseArray, rhs: &dyn BaseArray, func) -> Series {

// }

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

impl Add for &Series {
    type Output = Series;
    fn add(self, rhs: Self) -> Self::Output {
        let lhs = self;
        let supertype = get_supertype(lhs.data_type(), rhs.data_type()).unwrap();
        let lhs = self.cast(&supertype).unwrap();
        let rhs = rhs.cast(&supertype).unwrap();
        println!(
            "supertype: {:?} {:?} {:?}",
            supertype,
            lhs.data_type(),
            rhs.data_type()
        );
        with_match_arrow_daft_types!(supertype, |$T| {
            let lhs = lhs.downcast::<$T>().unwrap();
            let rhs = rhs.downcast::<$T>().unwrap();
            lhs.add(rhs).into_series()
        })
    }
}

mod tests {
    use crate::{
        array::DataArray,
        datatypes::{DateArray, Float64Array, Int64Array},
        error::DaftResult,
    };

    use super::*;

    #[test]
    fn add_int_and_float() -> DaftResult<()> {
        let a = Int64Array::from(vec![1, 2, 3].as_slice());
        println!("{:?}", a.data_type());
        let b = Float64Array::from(vec![1., 2., 3.].as_slice());
        let c = &a.into_series() + &b.into_series();

        Ok(())
    }
}
