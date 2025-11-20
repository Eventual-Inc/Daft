#[macro_export]
macro_rules! with_match_daft_types {
    (
        $key_type:expr
        , |$_:tt $T:ident| $($body:tt)*
        $(,)?
    ) => {{
        macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
        use $crate::datatypes::*;
        use $crate::datatypes::DataType;
        use $crate::file::{MediaType, UnknownFileType, VideoFileType, AudioFileType};

        match $key_type {
            // Float16 => unimplemented!("Array for Float16 DataType not implemented"),
            DataType::Binary => __with_ty__! { BinaryType },
            DataType::Boolean => __with_ty__! { BooleanType },
            DataType::Date => __with_ty__! { DateType },
            DataType::Decimal128(..) => __with_ty__! { Decimal128Type },
            DataType::Duration(_) => __with_ty__! { DurationType },
            DataType::Embedding(..) => __with_ty__! { EmbeddingType },
            DataType::Extension(_, _, _) => __with_ty__! { ExtensionType },
            DataType::FixedShapeImage(..) => __with_ty__! { FixedShapeImageType },
            DataType::FixedShapeSparseTensor(..) => __with_ty__! { FixedShapeSparseTensorType },
            DataType::FixedShapeTensor(..) => __with_ty__! { FixedShapeTensorType },
            DataType::FixedSizeBinary(_) => __with_ty__! { FixedSizeBinaryType },
            DataType::FixedSizeList(_, _) => __with_ty__! { FixedSizeListType },
            DataType::Float32 => __with_ty__! { Float32Type },
            DataType::Float64 => __with_ty__! { Float64Type },
            DataType::Image(..) => __with_ty__! { ImageType },
            DataType::Int16 => __with_ty__! { Int16Type },
            DataType::Int32 => __with_ty__! { Int32Type },
            DataType::Int64 => __with_ty__! { Int64Type },
            DataType::Int8 => __with_ty__! { Int8Type },
            DataType::Interval => __with_ty__! { IntervalType },
            DataType::List(_) => __with_ty__! { ListType },
            DataType::Map{..} => __with_ty__! { MapType },
            DataType::Null => __with_ty__! { NullType },
            DataType::SparseTensor(..) => __with_ty__! { SparseTensorType },
            DataType::Struct(_) => __with_ty__! { StructType },
            DataType::Tensor(..) => __with_ty__! { TensorType },
            DataType::Time(_) => __with_ty__! { TimeType },
            DataType::Timestamp(_, _) => __with_ty__! { TimestampType },
            DataType::UInt16 => __with_ty__! { UInt16Type },
            DataType::UInt32 => __with_ty__! { UInt32Type },
            DataType::UInt64 => __with_ty__! { UInt64Type },
            DataType::UInt8 => __with_ty__! { UInt8Type },
            DataType::Unknown => unimplemented!("array for unknown datatype not implemented"),
            DataType::Utf8 => __with_ty__! { Utf8Type },
            #[cfg(feature = "python")]
            DataType::Python => __with_ty__! { PythonType },
            DataType::File(MediaType::Unknown) => __with_ty__! { UnknownFileType },
            DataType::File(MediaType::Video) => __with_ty__! { VideoFileType },
            DataType::File(MediaType::Audio) => __with_ty__! { AudioFileType },


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
        use $crate::datatypes::DataType;

        match $key_type {
            DataType::Null => __with_ty__! { NullType },
            DataType::Boolean => __with_ty__! { BooleanType },
            DataType::Int8 => __with_ty__! { Int8Type },
            DataType::Int16 => __with_ty__! { Int16Type },
            DataType::Int32 => __with_ty__! { Int32Type },
            DataType::Int64 => __with_ty__! { Int64Type },
            DataType::Decimal128(_, _) => __with_ty__! { Decimal128Type },
            DataType::UInt8 => __with_ty__! { UInt8Type },
            DataType::UInt16 => __with_ty__! { UInt16Type },
            DataType::UInt32 => __with_ty__! { UInt32Type },
            DataType::UInt64 => __with_ty__! { UInt64Type },
            DataType::Float32 => __with_ty__! { Float32Type },
            DataType::Float64 => __with_ty__! { Float64Type },
            DataType::Binary => __with_ty__! { BinaryType },
            DataType::FixedSizeBinary(_) => __with_ty__! { FixedSizeBinaryType },
            DataType::Utf8 => __with_ty__! { Utf8Type },
            DataType::FixedSizeList(_, _) => __with_ty__! { FixedSizeListType },
            DataType::List(_) => __with_ty__! { ListType },
            DataType::Struct(_) => __with_ty__! { StructType },
            DataType::Extension(_, _, _) => __with_ty__! { ExtensionType },
            DataType::Interval => __with_ty__! { IntervalType },
            #[cfg(feature = "python")]
            DataType::Python => __with_ty__! { PythonType },

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
        use $crate::datatypes::DataType;

        match $key_type {
            DataType::Null => __with_ty__! { NullType },
            DataType::Boolean => __with_ty__! { BooleanType },
            DataType::Binary => __with_ty__! { BinaryType },
            DataType::FixedSizeBinary(_) => __with_ty__! { FixedSizeBinaryType },
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
            DataType::Decimal128(..) => __with_ty__! { Decimal128Type },
            DataType::List(_) => __with_ty__! { ListType },
            DataType::Extension(_, _, _) => __with_ty__! { ExtensionType },
            DataType::Utf8 => __with_ty__! { Utf8Type },

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
        use $crate::datatypes::DataType;

        match $key_type {
            DataType::Null => __with_ty__! { NullType },
            DataType::Boolean => __with_ty__! { BooleanType },
            DataType::Int8 => __with_ty__! { Int8Type },
            DataType::Int16 => __with_ty__! { Int16Type },
            DataType::Int32 => __with_ty__! { Int32Type },
            DataType::Int64 => __with_ty__! { Int64Type },
            DataType::Decimal128(..) => __with_ty__! { Decimal128Type },
            DataType::UInt8 => __with_ty__! { UInt8Type },
            DataType::UInt16 => __with_ty__! { UInt16Type },
            DataType::UInt32 => __with_ty__! { UInt32Type },
            DataType::UInt64 => __with_ty__! { UInt64Type },
            DataType::Float32 => __with_ty__! { Float32Type },
            DataType::Float64 => __with_ty__! { Float64Type },
            DataType::Utf8 => __with_ty__! { Utf8Type },
            DataType::Binary => __with_ty__! { BinaryType },
            DataType::FixedSizeBinary(_) => __with_ty__! { FixedSizeBinaryType },

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
        use $crate::datatypes::DataType;

        match $key_type {
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
        use $crate::datatypes::DataType;

        match $key_type {
            DataType::Null => __with_ty__! { NullType },
            DataType::Boolean => __with_ty__! { BooleanType },
            DataType::Int8 => __with_ty__! { Int8Type },
            DataType::Int16 => __with_ty__! { Int16Type },
            DataType::Int32 => __with_ty__! { Int32Type },
            DataType::Int64 => __with_ty__! { Int64Type },
            DataType::Decimal128(..) => __with_ty__! { Decimal128Type },
            DataType::UInt8 => __with_ty__! { UInt8Type },
            DataType::UInt16 => __with_ty__! { UInt16Type },
            DataType::UInt32 => __with_ty__! { UInt32Type },
            DataType::UInt64 => __with_ty__! { UInt64Type },
            DataType::Float32 => __with_ty__! { Float32Type },
            DataType::Float64 => __with_ty__! { Float64Type },
            DataType::Utf8 => __with_ty__! { Utf8Type },
            DataType::Binary => __with_ty__! { BinaryType },
            DataType::FixedSizeBinary(_) => __with_ty__! { FixedSizeBinaryType },
            DataType::List(_) => __with_ty__! { ListType },
            DataType::FixedSizeList(_, _) => __with_ty__! { FixedSizeListType },
            DataType::Struct(_) => __with_ty__! { StructType },

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
        use $crate::datatypes::DataType;

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
        use $crate::datatypes::DataType;

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
            DataType::Decimal128(..) => __with_ty__! { Decimal128Type },

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
        use $crate::datatypes::DataType;

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
        use $crate::datatypes::DataType;

        match $key_type {
            DataType::Null => __with_ty__! { NullType },
            DataType::Float32 => __with_ty__! { Float32Type },
            DataType::Float64 => __with_ty__! { Float64Type },

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
        use $crate::datatypes::DataType;

        match $key_type {
            DataType::Decimal128(..) => __with_ty__! { i128 },
            DataType::Duration(..) => __with_ty__! { i64 },
            DataType::Date => __with_ty__! { i32 },
            DataType::Time(..) => __with_ty__! { i64 },
            DataType::Timestamp(..) => __with_ty__! { i64 },

            _ => panic!("no logical -> primitive conversion available for {:?}", $key_type)
        }
    }};
}

#[macro_export]
macro_rules! with_match_file_types {
    (
        $key_type:expr
        , |$_:tt $T:ident| $($body:tt)*
        $(,)?
    ) => {{
        macro_rules! __with_ty__ {( $_ $T:ident ) => ( $($body)* )}
        use $crate::datatypes::*;
        use $crate::datatypes::DataType;
        use $crate::file::{MediaType, MediaTypeUnknown, MediaTypeVideo, MediaTypeAudio};

        match $key_type {
            DataType::File(MediaType::Unknown) => __with_ty__! { MediaTypeUnknown },
            DataType::File(MediaType::Video) => __with_ty__! { MediaTypeVideo },
            DataType::File(MediaType::Audio) => __with_ty__! { MediaTypeAudio },
            _ => panic!("Only File Types are supported, {:?} not implemented", $key_type)
        }
    }};
}
