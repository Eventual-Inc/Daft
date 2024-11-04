#[macro_export]
macro_rules! with_match_daft_types {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::DataType::*;
    #[allow(unused_imports)]
    use $crate::datatypes::*;

    match $key_type {
        // Float16 => unimplemented!("Array for Float16 DataType not implemented"),
        Binary => __with_ty__! { BinaryType },
        Boolean => __with_ty__! { BooleanType },
        Date => __with_ty__! { DateType },
        Decimal128(..) => __with_ty__! { Decimal128Type },
        Duration(_) => __with_ty__! { DurationType },
        Embedding(..) => __with_ty__! { EmbeddingType },
        Extension(_, _, _) => __with_ty__! { ExtensionType },
        FixedShapeImage(..) => __with_ty__! { FixedShapeImageType },
        FixedShapeSparseTensor(..) => __with_ty__! { FixedShapeSparseTensorType },
        FixedShapeTensor(..) => __with_ty__! { FixedShapeTensorType },
        FixedSizeBinary(_) => __with_ty__! { FixedSizeBinaryType },
        FixedSizeList(_, _) => __with_ty__! { FixedSizeListType },
        Float32 => __with_ty__! { Float32Type },
        Float64 => __with_ty__! { Float64Type },
        Image(..) => __with_ty__! { ImageType },
        Int16 => __with_ty__! { Int16Type },
        Int32 => __with_ty__! { Int32Type },
        Int64 => __with_ty__! { Int64Type },
        Int8 => __with_ty__! { Int8Type },
        Interval => __with_ty__! { IntervalType },
        List(_) => __with_ty__! { ListType },
        Map{..} => __with_ty__! { MapType },
        Null => __with_ty__! { NullType },
        SparseTensor(..) => __with_ty__! { SparseTensorType },
        Struct(_) => __with_ty__! { StructType },
        Tensor(..) => __with_ty__! { TensorType },
        Time(_) => __with_ty__! { TimeType },
        Timestamp(_, _) => __with_ty__! { TimestampType },
        UInt16 => __with_ty__! { UInt16Type },
        UInt32 => __with_ty__! { UInt32Type },
        UInt64 => __with_ty__! { UInt64Type },
        UInt8 => __with_ty__! { UInt8Type },
        Unknown => unimplemented!("Array for Unknown DataType not implemented"),
        Utf8 => __with_ty__! { Utf8Type },
        #[cfg(feature = "python")]
        Python => __with_ty__! { PythonType },

        // NOTE: We should not implement a default for match here, because this is meant to be
        // an exhaustive match across **all** Daft types.
        // _ => panic!("{:?} not implemented for with_match_daft_types", $key_type)
    }
})}
#[macro_export]
macro_rules! with_match_physical_daft_types {(
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
        Decimal128(_, _) => __with_ty__! { Decimal128Type },
        UInt8 => __with_ty__! { UInt8Type },
        UInt16 => __with_ty__! { UInt16Type },
        UInt32 => __with_ty__! { UInt32Type },
        UInt64 => __with_ty__! { UInt64Type },
        // Float16 => __with_ty__! { Float16Type },
        Float32 => __with_ty__! { Float32Type },
        Float64 => __with_ty__! { Float64Type },
        Binary => __with_ty__! { BinaryType },
        FixedSizeBinary(_) => __with_ty__! { FixedSizeBinaryType },
        Utf8 => __with_ty__! { Utf8Type },
        FixedSizeList(_, _) => __with_ty__! { FixedSizeListType },
        List(_) => __with_ty__! { ListType },
        Struct(_) => __with_ty__! { StructType },
        Extension(_, _, _) => __with_ty__! { ExtensionType },
        Interval => __with_ty__! { IntervalType },
        #[cfg(feature = "python")]
        Python => __with_ty__! { PythonType },
        _ => panic!("{:?} not implemented for with_match_physical_daft_types", $key_type)
    }
})}

#[macro_export]
macro_rules! with_match_arrow_daft_types {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::DataType::*;
    use $crate::datatypes::*;

    match $key_type {
        Null => __with_ty__! { NullType },
        Boolean => __with_ty__! { BooleanType },
        Binary => __with_ty__! { BinaryType },
        FixedSizeBinary(_) => __with_ty__! { FixedSizeBinaryType },
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
        Decimal128(..) => __with_ty__! { Decimal128Type },

        // Date => __with_ty__! { DateType },
        // Timestamp(_, _) => __with_ty__! { TimestampType },
        List(_) => __with_ty__! { ListType },
        Extension(_, _, _) => __with_ty__! { ExtensionType },
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
        Decimal128(..) => __with_ty__! { Decimal128Type },
        UInt8 => __with_ty__! { UInt8Type },
        UInt16 => __with_ty__! { UInt16Type },
        UInt32 => __with_ty__! { UInt32Type },
        UInt64 => __with_ty__! { UInt64Type },
        // Float16 => __with_ty__! { Float16Type },
        Float32 => __with_ty__! { Float32Type },
        Float64 => __with_ty__! { Float64Type },
        Utf8 => __with_ty__! { Utf8Type },
        Binary => __with_ty__! { BinaryType },
        FixedSizeBinary(_) => __with_ty__! { FixedSizeBinaryType },
        _ => panic!("{:?} not implemented", $key_type)
    }
})}

#[macro_export]
macro_rules! with_match_hashable_daft_types {(
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
        Decimal128(..) => __with_ty__! { Decimal128Type },
        UInt8 => __with_ty__! { UInt8Type },
        UInt16 => __with_ty__! { UInt16Type },
        UInt32 => __with_ty__! { UInt32Type },
        UInt64 => __with_ty__! { UInt64Type },
        // Float16 => __with_ty__! { Float16Type },
        Float32 => __with_ty__! { Float32Type },
        Float64 => __with_ty__! { Float64Type },
        Utf8 => __with_ty__! { Utf8Type },
        Binary => __with_ty__! { BinaryType },
        FixedSizeBinary(_) => __with_ty__! { FixedSizeBinaryType },
        List(_) => __with_ty__! { ListType },
        FixedSizeList(_, _) => __with_ty__! { FixedSizeListType },
        Struct(_) => __with_ty__! { StructType },
        _ => panic!("{:?} not implemented", $key_type)
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
macro_rules! with_match_primitive_daft_types {(
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
        Decimal128(..) => __with_ty__! { Decimal128Type },
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

#[macro_export]
macro_rules! with_match_float_and_null_daft_types {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::DataType::*;
    use $crate::datatypes::*;

    match $key_type {
        Null => __with_ty__! { NullType },
        // Float16 => __with_ty__! { Float16Type },
        Float32 => __with_ty__! { Float32Type },
        Float64 => __with_ty__! { Float64Type },
        _ => panic!("Only Float Types are supported, {:?} not implemented", $key_type)
    }
})}

#[macro_export]
macro_rules! with_match_daft_logical_primitive_types {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::DataType::*;
    match $key_type {
        Decimal128(..) => __with_ty__! { i128 },
        Duration(..) => __with_ty__! { i64 },
        Date => __with_ty__! { i32 },
        Time(..) => __with_ty__! { i64 },
        Timestamp(..) => __with_ty__! { i64 },
        _ => panic!("no logical -> primitive conversion available for {:?}", $key_type)
    }
})}
