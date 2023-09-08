mod binary_ops;
mod dtype;
mod field;
mod image_format;
mod image_mode;
mod matching;
mod time_unit;

use std::ops::{Add, Div, Mul, Rem, Sub};

pub use crate::array::{DataArray, FixedSizeListArray};
use crate::array::{ListArray, StructArray};
use arrow2::{
    compute::comparison::Simd8,
    types::{simd::Simd, NativeType},
};
pub use binary_ops::try_physical_supertype;
pub use dtype::DataType;
pub use field::Field;
pub use field::FieldID;
pub use image_format::ImageFormat;
pub use image_mode::ImageMode;
use num_traits::{Bounded, Float, FromPrimitive, Num, NumCast, ToPrimitive, Zero};
pub use time_unit::TimeUnit;

pub mod logical;

/// Trait that is implemented by all Array types
///
/// NOTE: Arrays are 'static because they fully own all their internal components
/// This implicitly allows them to implement the std::any::Any trait, which we rely on
/// for downcasting Series to concrete DaftArrayType types.
pub trait DaftArrayType: Clone + 'static {}

/// Trait to wrap DataType Enum
///
/// NOTE: DaftDataType is 'static because they are used in various Array implementations
/// as PhantomData<T>. These Array implementations need to be 'static (see: [`DaftArrayType`]).
/// This should not be a problem as [`DaftDataType`] are all defined as empty structs that own nothing.
pub trait DaftDataType: Sync + Send + Clone + 'static {
    // Concrete ArrayType that backs data of this DataType
    type ArrayType: DaftArrayType;

    // returns Daft DataType Enum
    fn get_dtype() -> DataType
    where
        Self: Sized;
}

pub trait DaftPhysicalType: Send + Sync + DaftDataType {}

pub trait DaftArrowBackedType: Send + Sync + DaftPhysicalType + 'static {}

pub trait DaftLogicalType: Send + Sync + DaftDataType + 'static {
    type PhysicalType: DaftPhysicalType;
}

macro_rules! impl_daft_arrow_datatype {
    ($ca:ident, $variant:ident) => {
        #[derive(Clone)]
        pub struct $ca {}

        impl DaftDataType for $ca {
            #[inline]
            fn get_dtype() -> DataType {
                DataType::$variant
            }

            type ArrayType = DataArray<$ca>;
        }

        impl DaftArrowBackedType for $ca {}
        impl DaftPhysicalType for $ca {}
    };
}

macro_rules! impl_daft_non_arrow_datatype {
    ($ca:ident, $variant:ident) => {
        #[derive(Clone)]
        pub struct $ca {}

        impl DaftDataType for $ca {
            #[inline]
            fn get_dtype() -> DataType {
                DataType::$variant
            }

            type ArrayType = DataArray<$ca>;
        }
        impl DaftPhysicalType for $ca {}
    };
}

macro_rules! impl_daft_logical_data_array_datatype {
    ($ca:ident, $variant:ident, $physical_type:ident) => {
        #[derive(Clone)]
        pub struct $ca {}

        impl DaftDataType for $ca {
            #[inline]
            fn get_dtype() -> DataType {
                DataType::$variant
            }

            type ArrayType = logical::LogicalArray<$ca>;
        }

        impl DaftLogicalType for $ca {
            type PhysicalType = $physical_type;
        }
    };
}

macro_rules! impl_daft_logical_fixed_size_list_datatype {
    ($ca:ident, $variant:ident) => {
        #[derive(Clone)]
        pub struct $ca {}

        impl DaftDataType for $ca {
            #[inline]
            fn get_dtype() -> DataType {
                DataType::$variant
            }

            type ArrayType = logical::LogicalArray<$ca>;
        }

        impl DaftLogicalType for $ca {
            type PhysicalType = FixedSizeListType;
        }
    };
}

macro_rules! impl_nested_datatype {
    ($ca:ident, $array_type:ident) => {
        #[derive(Clone)]
        pub struct $ca {}

        impl DaftDataType for $ca {
            #[inline]
            fn get_dtype() -> DataType {
                DataType::Unknown
            }

            type ArrayType = $array_type;
        }
        impl DaftPhysicalType for $ca {}
    };
}

impl_daft_arrow_datatype!(NullType, Null);
impl_daft_arrow_datatype!(BooleanType, Boolean);
impl_daft_arrow_datatype!(Int8Type, Int8);
impl_daft_arrow_datatype!(Int16Type, Int16);
impl_daft_arrow_datatype!(Int32Type, Int32);
impl_daft_arrow_datatype!(Int64Type, Int64);
impl_daft_arrow_datatype!(Int128Type, Int128);
impl_daft_arrow_datatype!(UInt8Type, UInt8);
impl_daft_arrow_datatype!(UInt16Type, UInt16);
impl_daft_arrow_datatype!(UInt32Type, UInt32);
impl_daft_arrow_datatype!(UInt64Type, UInt64);
// impl_daft_arrow_datatype!(Float16Type, Float16);
impl_daft_arrow_datatype!(Float32Type, Float32);
impl_daft_arrow_datatype!(Float64Type, Float64);
impl_daft_arrow_datatype!(BinaryType, Binary);
impl_daft_arrow_datatype!(Utf8Type, Utf8);
impl_daft_arrow_datatype!(ExtensionType, Unknown);

impl_nested_datatype!(FixedSizeListType, FixedSizeListArray);
impl_nested_datatype!(StructType, StructArray);
impl_nested_datatype!(ListType, ListArray);

impl_daft_logical_data_array_datatype!(Decimal128Type, Unknown, Int128Type);
impl_daft_logical_data_array_datatype!(TimestampType, Unknown, Int64Type);
impl_daft_logical_data_array_datatype!(DateType, Date, Int32Type);
// impl_daft_logical_data_array_datatype!(TimeType, Unknown, Int64Type);
impl_daft_logical_data_array_datatype!(DurationType, Unknown, Int64Type);
impl_daft_logical_data_array_datatype!(ImageType, Unknown, StructType);
impl_daft_logical_data_array_datatype!(TensorType, Unknown, StructType);
impl_daft_logical_fixed_size_list_datatype!(EmbeddingType, Unknown);
impl_daft_logical_fixed_size_list_datatype!(FixedShapeImageType, Unknown);
impl_daft_logical_fixed_size_list_datatype!(FixedShapeTensorType, Unknown);

#[cfg(feature = "python")]
impl_daft_non_arrow_datatype!(PythonType, Python);

pub trait NumericNative:
    PartialOrd
    + NativeType
    + Num
    + NumCast
    + Zero
    + Simd
    + Simd8
    + std::iter::Sum<Self>
    + Add<Output = Self>
    + Sub<Output = Self>
    + Mul<Output = Self>
    + Div<Output = Self>
    + Rem<Output = Self>
    + Bounded
    + FromPrimitive
    + ToPrimitive
{
    type DAFTTYPE: DaftNumericType;
}

/// Trait to express types that are native and can be vectorized
pub trait DaftNumericType: Send + Sync + DaftArrowBackedType + 'static {
    type Native: NumericNative;
}

impl NumericNative for i8 {
    type DAFTTYPE = Int8Type;
}
impl NumericNative for i16 {
    type DAFTTYPE = Int16Type;
}
impl NumericNative for i32 {
    type DAFTTYPE = Int32Type;
}
impl NumericNative for i64 {
    type DAFTTYPE = Int64Type;
}
impl NumericNative for i128 {
    type DAFTTYPE = Int128Type;
}
impl NumericNative for u8 {
    type DAFTTYPE = UInt8Type;
}
impl NumericNative for u16 {
    type DAFTTYPE = UInt16Type;
}
impl NumericNative for u32 {
    type DAFTTYPE = UInt32Type;
}
impl NumericNative for u64 {
    type DAFTTYPE = UInt64Type;
}

impl NumericNative for f32 {
    type DAFTTYPE = Float32Type;
}
impl NumericNative for f64 {
    type DAFTTYPE = Float64Type;
}

impl DaftNumericType for UInt8Type {
    type Native = u8;
}
impl DaftNumericType for UInt16Type {
    type Native = u16;
}
impl DaftNumericType for UInt32Type {
    type Native = u32;
}
impl DaftNumericType for UInt64Type {
    type Native = u64;
}
impl DaftNumericType for Int8Type {
    type Native = i8;
}
impl DaftNumericType for Int16Type {
    type Native = i16;
}
impl DaftNumericType for Int32Type {
    type Native = i32;
}
impl DaftNumericType for Int64Type {
    type Native = i64;
}
impl DaftNumericType for Int128Type {
    type Native = i128;
}
impl DaftNumericType for Float32Type {
    type Native = f32;
}
impl DaftNumericType for Float64Type {
    type Native = f64;
}

pub trait DaftIntegerType: DaftNumericType
where
    Self::Native: Ord,
{
}

impl DaftIntegerType for UInt8Type {}
impl DaftIntegerType for UInt16Type {}
impl DaftIntegerType for UInt32Type {}
impl DaftIntegerType for UInt64Type {}
impl DaftIntegerType for Int8Type {}
impl DaftIntegerType for Int16Type {}
impl DaftIntegerType for Int32Type {}
impl DaftIntegerType for Int64Type {}
impl DaftIntegerType for Int128Type {}

pub trait DaftFloatType: DaftNumericType
where
    Self::Native: Float,
{
}

impl DaftFloatType for Float32Type {}
impl DaftFloatType for Float64Type {}

pub trait DaftListLikeType: DaftDataType {}

impl DaftListLikeType for ListType {}
impl DaftListLikeType for FixedSizeListType {}

pub type NullArray = DataArray<NullType>;
pub type BooleanArray = DataArray<BooleanType>;
pub type Int8Array = DataArray<Int8Type>;
pub type Int16Array = DataArray<Int16Type>;
pub type Int32Array = DataArray<Int32Type>;
pub type Int64Array = DataArray<Int64Type>;
pub type Int128Array = DataArray<Int128Type>;
pub type UInt8Array = DataArray<UInt8Type>;
pub type UInt16Array = DataArray<UInt16Type>;
pub type UInt32Array = DataArray<UInt32Type>;
pub type UInt64Array = DataArray<UInt64Type>;
// pub type Float16Array = DataArray<Float16Type>;
pub type Float32Array = DataArray<Float32Type>;
pub type Float64Array = DataArray<Float64Type>;
pub type BinaryArray = DataArray<BinaryType>;
pub type Utf8Array = DataArray<Utf8Type>;
pub type ExtensionArray = DataArray<ExtensionType>;

#[cfg(feature = "python")]
pub type PythonArray = DataArray<PythonType>;
