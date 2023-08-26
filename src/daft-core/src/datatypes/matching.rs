#[macro_export]
macro_rules! with_match_daft_types {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::DataType;
    use $crate::datatypes::*;

    match $key_type {
        DataType::Null => __with_ty__! { NullType },
        DataType::Boolean => __with_ty__! { BooleanType },
        DataType::Int8 => __with_ty__! { Int8Type },
        DataType::Int16 => __with_ty__! { Int16Type },
        DataType::Int32 => __with_ty__! { Int32Type },
        DataType::Int64 => __with_ty__! { Int64Type },
        DataType::Int128(..) => __with_ty__! { Int128Type },
        DataType::UInt8 => __with_ty__! { UInt8Type },
        DataType::UInt16 => __with_ty__! { UInt16Type },
        DataType::UInt32 => __with_ty__! { UInt32Type },
        DataType::UInt64 => __with_ty__! { UInt64Type },
        DataType::Float32 => __with_ty__! { Float32Type },
        DataType::Float64 => __with_ty__! { Float64Type },
        DataType::Timestamp(_, _) => __with_ty__! { TimestampType },
        DataType::Date => __with_ty__! { DateType },
        DataType::Time(_) => __with_ty__! { TimeType },
        DataType::Duration(_) => __with_ty__! { DurationType },
        DataType::Binary => __with_ty__! { BinaryType },
        DataType::Utf8 => __with_ty__! { Utf8Type },
        DataType::FixedSizeList(_, _) => __with_ty__! { FixedSizeListType },
        DataType::List(_) => __with_ty__! { ListType },
        DataType::Struct(_) => __with_ty__! { StructType },
        DataType::Extension(_, _, _) => __with_ty__! { ExtensionType },
        #[cfg(feature = "python")]
        DataType::Python => __with_ty__! { PythonType },
        DataType::Embedding(..) => __with_ty__! { EmbeddingType },
        DataType::Image(..) => __with_ty__! { ImageType },
        DataType::FixedShapeImage(..) => __with_ty__! { FixedShapeImageType },
        DataType::Tensor(..) => __with_ty__! { TensorType },
        DataType::FixedShapeTensor(..) => __with_ty__! { FixedShapeTensorType },
        DataType::Decimal128(..) => __with_ty__! { Decimal128Type },
        DataType::Float16 => unimplemented!("Array for Float16 DataType not implemented"),
        DataType::Unknown => unimplemented!("Array for Unknown DataType not implemented"),

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
    use $crate::datatypes::DataType;
    use $crate::datatypes::*;

    match $key_type {
        DataType::Null => __with_ty__! { NullType },
        DataType::Boolean => __with_ty__! { BooleanType },
        DataType::Int8 => __with_ty__! { Int8Type },
        DataType::Int16 => __with_ty__! { Int16Type },
        DataType::Int32 => __with_ty__! { Int32Type },
        DataType::Int64 => __with_ty__! { Int64Type },
        DataType::UInt8 => __with_ty__! { UInt8Type },
        DataType::UInt16 => __with_ty__! { UInt16Type },
        DataType::UInt32 => __with_ty__! { UInt32Type },
        DataType::UInt64 => __with_ty__! { UInt64Type },
        DataType::Float32 => __with_ty__! { Float32Type },
        DataType::Float64 => __with_ty__! { Float64Type },
        DataType::Binary => __with_ty__! { BinaryType },
        DataType::Utf8 => __with_ty__! { Utf8Type },
        DataType::FixedSizeList(_, _) => __with_ty__! { FixedSizeListType },
        DataType::List(_) => __with_ty__! { ListType },
        DataType::Struct(_) => __with_ty__! { StructType },
        DataType::Extension(_, _, _) => __with_ty__! { ExtensionType },
        #[cfg(feature = "python")]
        DataType::Python => __with_ty__! { PythonType },
        _ => panic!("{:?} not implemented for with_match_physical_daft_types", $key_type)
    }
})}

#[macro_export]
macro_rules! with_match_arrow_daft_types {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::DataType;
    use $crate::datatypes::*;

    match $key_type {
        DataType::Null => __with_ty__! { NullType },
        DataType::Boolean => __with_ty__! { BooleanType },
        DataType::Binary => __with_ty__! { BinaryType },
        DataType::Int8 => __with_ty__! { Int8Type },
        DataType::Int16 => __with_ty__! { Int16Type },
        DataType::Int32 => __with_ty__! { Int32Type },
        DataType::Int64 => __with_ty__! { Int64Type },
        DataType::UInt8 => __with_ty__! { UInt8Type },
        DataType::UInt16 => __with_ty__! { UInt16Type },
        DataType::UInt32 => __with_ty__! { UInt32Type },
        DataType::UInt64 => __with_ty__! { UInt64Type },
        DataType::Float32 => __with_ty__! { Float32Type },
        DataType::Float64 => __with_ty__! { Float64Type },
        DataType::List(_) => __with_ty__! { ListType },
        DataType::FixedSizeList(..) => __with_ty__! { FixedSizeListType },
        DataType::Struct(_) => __with_ty__! { StructType },
        DataType::Extension(_, _, _) => __with_ty__! { ExtensionType },
        DataType::Utf8 => __with_ty__! { Utf8Type },
        _ => panic!("{:?} not implemented", $key_type)
    }
})}

#[macro_export]
macro_rules! with_match_comparable_daft_types {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::DataType;
    use $crate::datatypes::*;

    match $key_type {
        DataType::Null => __with_ty__! { NullType },
        DataType::Boolean => __with_ty__! { BooleanType },
        DataType::Int8 => __with_ty__! { Int8Type },
        DataType::Int16 => __with_ty__! { Int16Type },
        DataType::Int32 => __with_ty__! { Int32Type },
        DataType::Int64 => __with_ty__! { Int64Type },
        DataType::UInt8 => __with_ty__! { UInt8Type },
        DataType::UInt16 => __with_ty__! { UInt16Type },
        DataType::UInt32 => __with_ty__! { UInt32Type },
        DataType::UInt64 => __with_ty__! { UInt64Type },
        DataType::Float32 => __with_ty__! { Float32Type },
        DataType::Float64 => __with_ty__! { Float64Type },
        DataType::Utf8 => __with_ty__! { Utf8Type },
        _ => panic!("{:?} not implemented", $key_type)
    }
})}

#[macro_export]
macro_rules! with_match_numeric_and_utf_daft_types {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::DataType;
    use $crate::datatypes::*;

    match $key_type {
        DataType::Int8 => __with_ty__! { Int8Type },
        DataType::Int16 => __with_ty__! { Int16Type },
        DataType::Int32 => __with_ty__! { Int32Type },
        DataType::Int64 => __with_ty__! { Int64Type },
        DataType::UInt8 => __with_ty__! { UInt8Type },
        DataType::UInt16 => __with_ty__! { UInt16Type },
        DataType::UInt32 => __with_ty__! { UInt32Type },
        DataType::UInt64 => __with_ty__! { UInt64Type },
        DataType::Float32 => __with_ty__! { Float32Type },
        DataType::Float64 => __with_ty__! { Float64Type },
        DataType::Utf8 => __with_ty__! { Utf8Type },
        _ => panic!("{:?} not implemented", $key_type)
    }
})}

#[macro_export]
macro_rules! with_match_numeric_daft_types {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::DataType;
    use $crate::datatypes::*;

    match $key_type {
        DataType::Int8 => __with_ty__! { Int8Type },
        DataType::Int16 => __with_ty__! { Int16Type },
        DataType::Int32 => __with_ty__! { Int32Type },
        DataType::Int64 => __with_ty__! { Int64Type },
        DataType::UInt8 => __with_ty__! { UInt8Type },
        DataType::UInt16 => __with_ty__! { UInt16Type },
        DataType::UInt32 => __with_ty__! { UInt32Type },
        DataType::UInt64 => __with_ty__! { UInt64Type },
        DataType::Float32 => __with_ty__! { Float32Type },
        DataType::Float64 => __with_ty__! { Float64Type },
        _ => panic!("{:?} not implemented", $key_type)
    }
})}

#[macro_export]
macro_rules! with_match_integer_daft_types {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::DataType;
    use $crate::datatypes::*;

    match $key_type {
        DataType::Int8 => __with_ty__! { Int8Type },
        DataType::Int16 => __with_ty__! { Int16Type },
        DataType::Int32 => __with_ty__! { Int32Type },
        DataType::Int64 => __with_ty__! { Int64Type },
        DataType::UInt8 => __with_ty__! { UInt8Type },
        DataType::UInt16 => __with_ty__! { UInt16Type },
        DataType::UInt32 => __with_ty__! { UInt32Type },
        DataType::UInt64 => __with_ty__! { UInt64Type },
        _ => panic!("Only Integer Types are supported, {:?} not implemented", $key_type)
    }
})}

#[macro_export]
macro_rules! with_match_float_and_null_daft_types {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::DataType;
    use $crate::datatypes::*;

    match $key_type {
        DataType::Null => __with_ty__! { NullType },
        // Float16 => __with_ty__! { Float16Type },
        DataType::Float32 => __with_ty__! { Float32Type },
        DataType::Float64 => __with_ty__! { Float64Type },
        _ => panic!("Only Float Types are supported, {:?} not implemented", $key_type)
    }
})}

#[macro_export]
macro_rules! with_match_daft_logical_types {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::DataType;
    use $crate::datatypes::*;

    match $key_type {
        DataType::Decimal128(..) => __with_ty__! { Decimal128Type },
        DataType::Date => __with_ty__! { DateType },
        DataType::Duration(..) => __with_ty__! { DurationType },
        DataType::Timestamp(..) => __with_ty__! { TimestampType },
        DataType::Embedding(..) => __with_ty__! { EmbeddingType },
        DataType::Image(..) => __with_ty__! { ImageType },
        DataType::FixedShapeImage(..) => __with_ty__! { FixedShapeImageType },
        DataType::Tensor(..) => __with_ty__! { TensorType },
        DataType::FixedShapeTensor(..) => __with_ty__! { FixedShapeTensorType },
        _ => panic!("{:?} not implemented for with_match_daft_logical_types", $key_type)
    }
})}

#[macro_export]
macro_rules! with_match_daft_logical_primitive_types {(
    $key_type:expr, | $_:tt $T:ident | $($body:tt)*
) => ({
    macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
    use $crate::datatypes::DataType;
    match $key_type {
        DataType::Decimal128(..) => __with_ty__! { i128 },
        DataType::Duration(..) => __with_ty__! { i64 },
        DataType::Date => __with_ty__! { i32 },
        DataType::Timestamp(..) => __with_ty__! { i64 },
        _ => panic!("no logical -> primitive conversion available for {:?}", $key_type)
    }
})}
