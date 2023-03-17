use crate::error::DaftResult;
use crate::utils::supertype::try_get_supertype;

use super::Series;

pub mod abs;
pub mod agg;
pub mod arithmetic;
pub mod broadcast;
pub mod cast;
pub mod comparision;
pub mod concat;
pub mod date;
pub mod downcast;
pub mod filter;
pub mod full;
pub mod hash;
pub mod len;
pub mod pairwise;
pub mod search_sorted;
pub mod sort;
pub mod take;
pub mod utf8;

fn match_types_on_series(l: &Series, r: &Series) -> DaftResult<(Series, Series)> {
    let supertype = try_get_supertype(l.data_type(), r.data_type())?;

    let mut lhs = l.clone();

    let mut rhs = r.clone();

    if !lhs.data_type().eq(&supertype) {
        lhs = lhs.cast(&supertype)?;
    }
    if !rhs.data_type().eq(&supertype) {
        rhs = rhs.cast(&supertype)?;
    }

    Ok((lhs, rhs))
}

#[macro_export]
macro_rules! with_match_daft_types {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::DataType::*;
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
        Float16 => __with_ty__! { Float16Type },
        Float32 => __with_ty__! { Float32Type },
        Float64 => __with_ty__! { Float64Type },
        Timestamp(_, _) => __with_ty__! { TimestampType },
        Date => __with_ty__! { DateType },
        Time(_) => __with_ty__! { TimeType },
        Duration(_) => __with_ty__! { DurationType },
        Binary => __with_ty__! { BinaryType },
        Utf8 => __with_ty__! { Utf8Type },
        FixedSizeList(_, _) => __with_ty__! { FixedSizeListType },
        List(_) => __with_ty__! { ListType },
        Struct(_) => __with_ty__! { StructType },
        _ => panic!("{:?} not implemented for with_match_daft_types", $key_type)
    }
})}

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
        _ => panic!("Only Integer Types are supported, {:?} not implemented", $key_type)
    }
})}
