#[macro_export]
macro_rules! with_match_daft_types {
    (
        $key_type:expr
        , |$_:tt $T:ident| $($body:tt)*
        $(,)?
    ) => {{
        macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
        use $crate::datatypes::*;

        match $key_type {
            // Float16 => unimplemented!("Array for Float16 DataType not implemented"),
            $crate::datatypes::DataType::Binary => __with_ty__! { BinaryType },
            $crate::datatypes::DataType::Boolean => __with_ty__! { BooleanType },
            $crate::datatypes::DataType::Date => __with_ty__! { DateType },
            $crate::datatypes::DataType::Decimal128(..) => __with_ty__! { Decimal128Type },
            $crate::datatypes::DataType::Duration(_) => __with_ty__! { DurationType },
            $crate::datatypes::DataType::Embedding(..) => __with_ty__! { EmbeddingType },
            $crate::datatypes::DataType::Extension(_, _, _) => __with_ty__! { ExtensionType },
            $crate::datatypes::DataType::FixedShapeImage(..) => __with_ty__! { FixedShapeImageType },
            $crate::datatypes::DataType::FixedShapeSparseTensor(..) => __with_ty__! { FixedShapeSparseTensorType },
            $crate::datatypes::DataType::FixedShapeTensor(..) => __with_ty__! { FixedShapeTensorType },
            $crate::datatypes::DataType::FixedSizeBinary(_) => __with_ty__! { FixedSizeBinaryType },
            $crate::datatypes::DataType::FixedSizeList(_, _) => __with_ty__! { FixedSizeListType },
            $crate::datatypes::DataType::Float32 => __with_ty__! { Float32Type },
            $crate::datatypes::DataType::Float64 => __with_ty__! { Float64Type },
            $crate::datatypes::DataType::Image(..) => __with_ty__! { ImageType },
            $crate::datatypes::DataType::Int16 => __with_ty__! { Int16Type },
            $crate::datatypes::DataType::Int32 => __with_ty__! { Int32Type },
            $crate::datatypes::DataType::Int64 => __with_ty__! { Int64Type },
            $crate::datatypes::DataType::Int8 => __with_ty__! { Int8Type },
            $crate::datatypes::DataType::Interval => __with_ty__! { IntervalType },
            $crate::datatypes::DataType::List(_) => __with_ty__! { ListType },
            $crate::datatypes::DataType::Map{..} => __with_ty__! { MapType },
            $crate::datatypes::DataType::Null => __with_ty__! { NullType },
            $crate::datatypes::DataType::SparseTensor(..) => __with_ty__! { SparseTensorType },
            $crate::datatypes::DataType::Struct(_) => __with_ty__! { StructType },
            $crate::datatypes::DataType::Tensor(..) => __with_ty__! { TensorType },
            $crate::datatypes::DataType::Time(_) => __with_ty__! { TimeType },
            $crate::datatypes::DataType::Timestamp(_, _) => __with_ty__! { TimestampType },
            $crate::datatypes::DataType::UInt16 => __with_ty__! { UInt16Type },
            $crate::datatypes::DataType::UInt32 => __with_ty__! { UInt32Type },
            $crate::datatypes::DataType::UInt64 => __with_ty__! { UInt64Type },
            $crate::datatypes::DataType::UInt8 => __with_ty__! { UInt8Type },
            $crate::datatypes::DataType::Unknown => unimplemented!("array for unknown datatype not implemented"),
            $crate::datatypes::DataType::Utf8 => __with_ty__! { Utf8Type },
            #[cfg(feature = "python")]
            $crate::datatypes::DataType::Python => __with_ty__! { PythonType },

            // NOTE: We should not implement a default for match here, because this is meant to be
            // an exhaustive match across **all** Daft types.
            // _ => panic!("{:?} not implemented for with_match_daft_types", $key_type)
        }
    }};
}

#[macro_export]
macro_rules! with_match_physical_daft_types {
    (
        $key_type:expr
        , |$_:tt $T:ident| $($body:tt)*
        $(,)?
    ) => {{
        macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
        use $crate::datatypes::*;

        match $key_type {
            $crate::datatypes::DataType::Null => __with_ty__! { NullType },
            $crate::datatypes::DataType::Boolean => __with_ty__! { BooleanType },
            $crate::datatypes::DataType::Int8 => __with_ty__! { Int8Type },
            $crate::datatypes::DataType::Int16 => __with_ty__! { Int16Type },
            $crate::datatypes::DataType::Int32 => __with_ty__! { Int32Type },
            $crate::datatypes::DataType::Int64 => __with_ty__! { Int64Type },
            $crate::datatypes::DataType::Decimal128(_, _) => __with_ty__! { Decimal128Type },
            $crate::datatypes::DataType::UInt8 => __with_ty__! { UInt8Type },
            $crate::datatypes::DataType::UInt16 => __with_ty__! { UInt16Type },
            $crate::datatypes::DataType::UInt32 => __with_ty__! { UInt32Type },
            $crate::datatypes::DataType::UInt64 => __with_ty__! { UInt64Type },
            $crate::datatypes::DataType::Float32 => __with_ty__! { Float32Type },
            $crate::datatypes::DataType::Float64 => __with_ty__! { Float64Type },
            $crate::datatypes::DataType::Binary => __with_ty__! { BinaryType },
            $crate::datatypes::DataType::FixedSizeBinary(_) => __with_ty__! { FixedSizeBinaryType },
            $crate::datatypes::DataType::Utf8 => __with_ty__! { Utf8Type },
            $crate::datatypes::DataType::FixedSizeList(_, _) => __with_ty__! { FixedSizeListType },
            $crate::datatypes::DataType::List(_) => __with_ty__! { ListType },
            $crate::datatypes::DataType::Struct(_) => __with_ty__! { StructType },
            $crate::datatypes::DataType::Extension(_, _, _) => __with_ty__! { ExtensionType },
            $crate::datatypes::DataType::Interval => __with_ty__! { IntervalType },
            #[cfg(feature = "python")]
            $crate::datatypes::DataType::Python => __with_ty__! { PythonType },
            _ => panic!("{:?} not implemented for with_match_physical_daft_types", $key_type)
        }
    }};
}

#[macro_export]
macro_rules! with_match_arrow_daft_types {
    (
        $key_type:expr
        , |$_:tt $T:ident| $($body:tt)*
        $(,)?
    ) => {{
        macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
        use $crate::datatypes::*;

        match $key_type {
            $crate::datatypes::DataType::Null => __with_ty__! { NullType },
            $crate::datatypes::DataType::Boolean => __with_ty__! { BooleanType },
            $crate::datatypes::DataType::Binary => __with_ty__! { BinaryType },
            $crate::datatypes::DataType::FixedSizeBinary(_) => __with_ty__! { FixedSizeBinaryType },
            $crate::datatypes::DataType::Int8 => __with_ty__! { Int8Type },
            $crate::datatypes::DataType::Int16 => __with_ty__! { Int16Type },
            $crate::datatypes::DataType::Int32 => __with_ty__! { Int32Type },
            $crate::datatypes::DataType::Int64 => __with_ty__! { Int64Type },
            $crate::datatypes::DataType::UInt8 => __with_ty__! { UInt8Type },
            $crate::datatypes::DataType::UInt16 => __with_ty__! { UInt16Type },
            $crate::datatypes::DataType::UInt32 => __with_ty__! { UInt32Type },
            $crate::datatypes::DataType::UInt64 => __with_ty__! { UInt64Type },
            $crate::datatypes::DataType::Float32 => __with_ty__! { Float32Type },
            $crate::datatypes::DataType::Float64 => __with_ty__! { Float64Type },
            $crate::datatypes::DataType::Decimal128(..) => __with_ty__! { Decimal128Type },
            $crate::datatypes::DataType::List(_) => __with_ty__! { ListType },
            $crate::datatypes::DataType::Extension(_, _, _) => __with_ty__! { ExtensionType },
            $crate::datatypes::DataType::Utf8 => __with_ty__! { Utf8Type },
            _ => panic!("{:?} not implemented", $key_type)
        }
    }};
}

#[macro_export]
macro_rules! with_match_comparable_daft_types {
    (
        $key_type:expr
        , |$_:tt $T:ident| $($body:tt)*
        $(,)?
    ) => {{
        macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
        use $crate::datatypes::*;

        match $key_type {
            $crate::datatypes::DataType::Null => __with_ty__! { NullType },
            $crate::datatypes::DataType::Boolean => __with_ty__! { BooleanType },
            $crate::datatypes::DataType::Int8 => __with_ty__! { Int8Type },
            $crate::datatypes::DataType::Int16 => __with_ty__! { Int16Type },
            $crate::datatypes::DataType::Int32 => __with_ty__! { Int32Type },
            $crate::datatypes::DataType::Int64 => __with_ty__! { Int64Type },
            $crate::datatypes::DataType::Decimal128(..) => __with_ty__! { Decimal128Type },
            $crate::datatypes::DataType::UInt8 => __with_ty__! { UInt8Type },
            $crate::datatypes::DataType::UInt16 => __with_ty__! { UInt16Type },
            $crate::datatypes::DataType::UInt32 => __with_ty__! { UInt32Type },
            $crate::datatypes::DataType::UInt64 => __with_ty__! { UInt64Type },
            $crate::datatypes::DataType::Float32 => __with_ty__! { Float32Type },
            $crate::datatypes::DataType::Float64 => __with_ty__! { Float64Type },
            $crate::datatypes::DataType::Utf8 => __with_ty__! { Utf8Type },
            $crate::datatypes::DataType::Binary => __with_ty__! { BinaryType },
            $crate::datatypes::DataType::FixedSizeBinary(_) => __with_ty__! { FixedSizeBinaryType },
            _ => panic!("{:?} not implemented", $key_type)
        }
    }};
}

#[macro_export]
macro_rules! with_match_iterable_daft_types {
    (
        $key_type:expr
        , |$_:tt $T:ident| $($body:tt)*
        $(,)?
    ) => {{
        macro_rules! __with_ty__ {($_ $T:ident) => {
                $($body)*
            };
        }
        use $crate::datatypes::*;

        match $key_type {
            $crate::datatypes::DataType::Boolean => __with_ty__! { BooleanType },
            $crate::datatypes::DataType::Int8 => __with_ty__! { Int8Type },
            $crate::datatypes::DataType::Int16 => __with_ty__! { Int16Type },
            $crate::datatypes::DataType::Int32 => __with_ty__! { Int32Type },
            $crate::datatypes::DataType::Int64 => __with_ty__! { Int64Type },
            $crate::datatypes::DataType::UInt8 => __with_ty__! { UInt8Type },
            $crate::datatypes::DataType::UInt16 => __with_ty__! { UInt16Type },
            $crate::datatypes::DataType::UInt32 => __with_ty__! { UInt32Type },
            $crate::datatypes::DataType::UInt64 => __with_ty__! { UInt64Type },
            $crate::datatypes::DataType::Float32 => __with_ty__! { Float32Type },
            $crate::datatypes::DataType::Float64 => __with_ty__! { Float64Type },
            _ => panic!("{:?} not implemented", $key_type),
        }
    }};
}

#[macro_export]
macro_rules! with_match_hashable_daft_types {
    (
        $key_type:expr
        , |$_:tt $T:ident| $($body:tt)*
        $(,)?
    ) => {{
        macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
        use $crate::datatypes::*;

        match $key_type {
            $crate::datatypes::DataType::Null => __with_ty__! { NullType },
            $crate::datatypes::DataType::Boolean => __with_ty__! { BooleanType },
            $crate::datatypes::DataType::Int8 => __with_ty__! { Int8Type },
            $crate::datatypes::DataType::Int16 => __with_ty__! { Int16Type },
            $crate::datatypes::DataType::Int32 => __with_ty__! { Int32Type },
            $crate::datatypes::DataType::Int64 => __with_ty__! { Int64Type },
            $crate::datatypes::DataType::Decimal128(..) => __with_ty__! { Decimal128Type },
            $crate::datatypes::DataType::UInt8 => __with_ty__! { UInt8Type },
            $crate::datatypes::DataType::UInt16 => __with_ty__! { UInt16Type },
            $crate::datatypes::DataType::UInt32 => __with_ty__! { UInt32Type },
            $crate::datatypes::DataType::UInt64 => __with_ty__! { UInt64Type },
            $crate::datatypes::DataType::Float32 => __with_ty__! { Float32Type },
            $crate::datatypes::DataType::Float64 => __with_ty__! { Float64Type },
            $crate::datatypes::DataType::Utf8 => __with_ty__! { Utf8Type },
            $crate::datatypes::DataType::Binary => __with_ty__! { BinaryType },
            $crate::datatypes::DataType::FixedSizeBinary(_) => __with_ty__! { FixedSizeBinaryType },
            $crate::datatypes::DataType::List(_) => __with_ty__! { ListType },
            $crate::datatypes::DataType::FixedSizeList(_, _) => __with_ty__! { FixedSizeListType },
            $crate::datatypes::DataType::Struct(_) => __with_ty__! { StructType },
            _ => panic!("{:?} not implemented", $key_type)
        }
    }};
}

#[macro_export]
macro_rules! with_match_numeric_daft_types {
    (
        $key_type:expr
        , |$_:tt $T:ident| $($body:tt)*
        $(,)?
    ) => {{
        macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
        use $crate::datatypes::*;

        match $key_type {
            $crate::datatypes::DataType::Int8 => __with_ty__! { Int8Type },
            $crate::datatypes::DataType::Int16 => __with_ty__! { Int16Type },
            $crate::datatypes::DataType::Int32 => __with_ty__! { Int32Type },
            $crate::datatypes::DataType::Int64 => __with_ty__! { Int64Type },
            $crate::datatypes::DataType::UInt8 => __with_ty__! { UInt8Type },
            $crate::datatypes::DataType::UInt16 => __with_ty__! { UInt16Type },
            $crate::datatypes::DataType::UInt32 => __with_ty__! { UInt32Type },
            $crate::datatypes::DataType::UInt64 => __with_ty__! { UInt64Type },
            $crate::datatypes::DataType::Float32 => __with_ty__! { Float32Type },
            $crate::datatypes::DataType::Float64 => __with_ty__! { Float64Type },
            _ => panic!("{:?} not implemented", $key_type)
        }
    }};
}

#[macro_export]
macro_rules! with_match_primitive_daft_types {
    (
        $key_type:expr
        , |$_:tt $T:ident| $($body:tt)*
        $(,)?
    ) => {{
        macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
        use $crate::datatypes::*;

        match $key_type {
            $crate::datatypes::DataType::Int8 => __with_ty__! { Int8Type },
            $crate::datatypes::DataType::Int16 => __with_ty__! { Int16Type },
            $crate::datatypes::DataType::Int32 => __with_ty__! { Int32Type },
            $crate::datatypes::DataType::Int64 => __with_ty__! { Int64Type },
            $crate::datatypes::DataType::UInt8 => __with_ty__! { UInt8Type },
            $crate::datatypes::DataType::UInt16 => __with_ty__! { UInt16Type },
            $crate::datatypes::DataType::UInt32 => __with_ty__! { UInt32Type },
            $crate::datatypes::DataType::UInt64 => __with_ty__! { UInt64Type },
            $crate::datatypes::DataType::Float32 => __with_ty__! { Float32Type },
            $crate::datatypes::DataType::Float64 => __with_ty__! { Float64Type },
            $crate::datatypes::DataType::Decimal128(..) => __with_ty__! { Decimal128Type },
            _ => panic!("{:?} not implemented", $key_type)
        }
    }};
}

#[macro_export]
macro_rules! with_match_integer_daft_types {
    (
        $key_type:expr
        , |$_:tt $T:ident| $($body:tt)*
        $(,)?
    ) => {{
        macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
        use $crate::datatypes::*;

        match $key_type {
            $crate::datatypes::DataType::Int8 => __with_ty__! { Int8Type },
            $crate::datatypes::DataType::Int16 => __with_ty__! { Int16Type },
            $crate::datatypes::DataType::Int32 => __with_ty__! { Int32Type },
            $crate::datatypes::DataType::Int64 => __with_ty__! { Int64Type },
            $crate::datatypes::DataType::UInt8 => __with_ty__! { UInt8Type },
            $crate::datatypes::DataType::UInt16 => __with_ty__! { UInt16Type },
            $crate::datatypes::DataType::UInt32 => __with_ty__! { UInt32Type },
            $crate::datatypes::DataType::UInt64 => __with_ty__! { UInt64Type },
            _ => panic!("Only Integer Types are supported, {:?} not implemented", $key_type)
        }
    }};
}

#[macro_export]
macro_rules! with_match_float_and_null_daft_types {
    (
        $key_type:expr
        , |$_:tt $T:ident| $($body:tt)*
        $(,)?
    ) => {{
        macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
        use $crate::datatypes::*;

        match $key_type {
            $crate::datatypes::DataType::Null => __with_ty__! { NullType },
            $crate::datatypes::DataType::Float32 => __with_ty__! { Float32Type },
            $crate::datatypes::DataType::Float64 => __with_ty__! { Float64Type },
            _ => panic!("Only Float Types are supported, {:?} not implemented", $key_type)
        }
    }};
}

#[macro_export]
macro_rules! with_match_daft_logical_primitive_types {
    (
        $key_type:expr
        , |$_:tt $T:ident| $($body:tt)*
        $(,)?
    ) => {{
        macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
        match $key_type {
            $crate::datatypes::DataType::Decimal128(..) => __with_ty__! { i128 },
            $crate::datatypes::DataType::Duration(..) => __with_ty__! { i64 },
            $crate::datatypes::DataType::Date => __with_ty__! { i32 },
            $crate::datatypes::DataType::Time(..) => __with_ty__! { i64 },
            $crate::datatypes::DataType::Timestamp(..) => __with_ty__! { i64 },
            _ => panic!("no logical -> primitive conversion available for {:?}", $key_type)
        }
    }};
}
