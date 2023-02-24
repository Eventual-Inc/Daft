use crate::{error::DaftResult, utils::supertype::try_get_supertype};

use super::Series;

pub mod arithmetic;
pub mod broadcast;
pub mod cast;
pub mod comparision;
pub mod downcast;
pub mod filter;
pub mod take;

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

#[macro_export]
macro_rules! with_match_comparable_daft_types {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::DataType::*;
    #[allow(unused_imports)]
    use $crate::datatypes::*;

    match $key_type {
        Null => __with_ty__! { NullType },
        Boolean => __with_ty__! { BooleanType },
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

#[macro_export]
macro_rules! with_match_integer_daft_types {(
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
        _ => panic!("{:?} not implemented", $key_type)
    }
})}
