use common_error::DaftResult;

use crate::{
    datatypes::{
        logical::LogicalArray, BinaryArray, BinaryType, BooleanArray, BooleanType, DaftLogicalType,
        DateType, Decimal128Type, DurationType, EmbeddingType, ExtensionArray, FixedShapeImageType,
        FixedShapeTensorType, FixedSizeListArray, FixedSizeListType, Float32Array, Float32Type,
        Float64Array, Float64Type, ImageType, Int128Array, Int128Type, Int16Array, Int16Type,
        Int32Array, Int32Type, Int64Array, Int64Type, Int8Array, Int8Type, ListArray, ListType,
        NullArray, NullType, StructArray, StructType, TensorType, TimestampType, UInt16Array,
        UInt16Type, UInt32Array, UInt32Type, UInt64Array, UInt64Type, UInt8Array, UInt8Type,
        Utf8Array, Utf8Type,
    },
    DataType, Series,
};

use super::{ops::as_arrow::AsArrow, DataArray};

mod arrow_growable;
mod logical_growable;

#[cfg(feature = "python")]
mod python_growable;
#[cfg(feature = "python")]
use crate::datatypes::PythonArray;

/// Describes a struct that can be extended from slices of other pre-existing Series.
/// This is very useful for abstracting many "physical" operations such as takes, broadcasts,
/// filters and more.
pub trait Growable {
    /// Extends this [`Growable`] with elements from the bounded [`Array`] at index `index` from
    /// a slice starting at `start` and length `len`.
    /// # Panic
    /// This function panics if the range is out of bounds, i.e. if `start + len >= array.len()`.
    fn extend(&mut self, index: usize, start: usize, len: usize);

    /// Extends this [`Growable`] with null elements
    fn add_nulls(&mut self, additional: usize);

    /// Builds an array from the [`Growable`]
    fn build(&mut self) -> DaftResult<Series>;
}

type ArrowNullGrowable<'a> =
    arrow_growable::ArrowGrowable<'a, NullType, arrow2::array::growable::GrowableNull>;
type ArrowBooleanGrowable<'a> =
    arrow_growable::ArrowGrowable<'a, BooleanType, arrow2::array::growable::GrowableBoolean<'a>>;
type ArrowInt8Growable<'a> =
    arrow_growable::ArrowGrowable<'a, Int8Type, arrow2::array::growable::GrowablePrimitive<'a, i8>>;
type ArrowInt16Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    Int16Type,
    arrow2::array::growable::GrowablePrimitive<'a, i16>,
>;
type ArrowInt32Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    Int32Type,
    arrow2::array::growable::GrowablePrimitive<'a, i32>,
>;
type ArrowInt64Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    Int64Type,
    arrow2::array::growable::GrowablePrimitive<'a, i64>,
>;
type ArrowInt128Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    Int128Type,
    arrow2::array::growable::GrowablePrimitive<'a, i128>,
>;
type ArrowUInt8Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    UInt8Type,
    arrow2::array::growable::GrowablePrimitive<'a, u8>,
>;
type ArrowUInt16Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    UInt16Type,
    arrow2::array::growable::GrowablePrimitive<'a, u16>,
>;
type ArrowUInt32Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    UInt32Type,
    arrow2::array::growable::GrowablePrimitive<'a, u32>,
>;
type ArrowUInt64Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    UInt64Type,
    arrow2::array::growable::GrowablePrimitive<'a, u64>,
>;
type ArrowFloat32Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    Float32Type,
    arrow2::array::growable::GrowablePrimitive<'a, f32>,
>;
type ArrowFloat64Growable<'a> = arrow_growable::ArrowGrowable<
    'a,
    Float64Type,
    arrow2::array::growable::GrowablePrimitive<'a, f64>,
>;
type ArrowBinaryGrowable<'a> =
    arrow_growable::ArrowGrowable<'a, BinaryType, arrow2::array::growable::GrowableBinary<'a, i64>>;
type ArrowUtf8Growable<'a> =
    arrow_growable::ArrowGrowable<'a, Utf8Type, arrow2::array::growable::GrowableUtf8<'a, i64>>;
type ArrowListGrowable<'a> =
    arrow_growable::ArrowGrowable<'a, ListType, arrow2::array::growable::GrowableList<'a, i64>>;
type ArrowFixedSizeListGrowable<'a> = arrow_growable::ArrowGrowable<
    'a,
    FixedSizeListType,
    arrow2::array::growable::GrowableFixedSizeList<'a>,
>;
type ArrowStructGrowable<'a> =
    arrow_growable::ArrowGrowable<'a, StructType, arrow2::array::growable::GrowableStruct<'a>>;

/// Trait that an Array type can implement to provide a Growable factory method
pub trait GrowableArray<'a, G: Growable> {
    fn make_growable(name: String, dtype: &DataType, arrays: Vec<&'a Self>, capacity: usize) -> G;
}

impl<'a> GrowableArray<'a, ArrowNullGrowable<'a>> for NullArray {
    fn make_growable(
        name: String,
        dtype: &DataType,
        _arrays: Vec<&Self>,
        _capacity: usize,
    ) -> ArrowNullGrowable<'a> {
        ArrowNullGrowable::new(
            name,
            dtype,
            arrow2::array::growable::GrowableNull::new(dtype.to_arrow().unwrap()),
        )
    }
}

#[cfg(feature = "python")]
impl<'a> GrowableArray<'a, python_growable::PythonGrowable<'a>> for PythonArray {
    fn make_growable(
        name: String,
        dtype: &DataType,
        arrays: Vec<&'a Self>,
        capacity: usize,
    ) -> python_growable::PythonGrowable<'a> {
        python_growable::PythonGrowable::new(name, dtype, arrays, capacity)
    }
}

impl<'a> GrowableArray<'a, arrow_growable::ArrowExtensionGrowable<'a>> for ExtensionArray {
    fn make_growable(
        name: String,
        dtype: &DataType,
        arrays: Vec<&'a Self>,
        capacity: usize,
    ) -> arrow_growable::ArrowExtensionGrowable<'a> {
        let downcasted_arrays = arrays.iter().map(|arr| arr.data()).collect::<Vec<_>>();
        let arrow2_growable =
            arrow2::array::growable::make_growable(downcasted_arrays.as_slice(), false, capacity);
        arrow_growable::ArrowExtensionGrowable::new(name, dtype, arrow2_growable)
    }
}

macro_rules! impl_primitive_growable_array {
    (
        $daft_array:ident,
        $growable:ident,
        $arrow_growable:ty
    ) => {
        impl<'a> GrowableArray<'a, $growable<'a>> for $daft_array {
            fn make_growable(
                name: String,
                dtype: &DataType,
                arrays: Vec<&'a Self>,
                capacity: usize,
            ) -> $growable<'a> {
                $growable::new(
                    name,
                    dtype,
                    <$arrow_growable>::new(
                        arrays.iter().map(|a| a.as_arrow()).collect::<Vec<_>>(),
                        false,
                        capacity,
                    ),
                )
            }
        }
    };
}

macro_rules! impl_logical_growable_array {
    (
        $daft_logical_type:ident
    ) => {
        impl<'a> GrowableArray<'a, logical_growable::LogicalGrowable<'a, $daft_logical_type>>
            for LogicalArray<$daft_logical_type>
        {
            fn make_growable(
                name: String,
                dtype: &DataType,
                arrays: Vec<&'a Self>,
                capacity: usize,
            ) -> logical_growable::LogicalGrowable<'a, $daft_logical_type> {
                logical_growable::LogicalGrowable::<$daft_logical_type>::new(
                    name.clone(),
                    dtype,
                    Box::new(DataArray::<
                        <$daft_logical_type as DaftLogicalType>::PhysicalType,
                    >::make_growable(
                        name,
                        &dtype.to_physical(),
                        arrays.iter().map(|a| &a.physical).collect::<Vec<_>>(),
                        capacity,
                    )),
                )
            }
        }
    };
}

impl_primitive_growable_array!(
    BooleanArray,
    ArrowBooleanGrowable,
    arrow2::array::growable::GrowableBoolean
);
impl_primitive_growable_array!(
    Int8Array,
    ArrowInt8Growable,
    arrow2::array::growable::GrowablePrimitive<i8>
);
impl_primitive_growable_array!(
    Int16Array,
    ArrowInt16Growable,
    arrow2::array::growable::GrowablePrimitive<i16>
);
impl_primitive_growable_array!(
    Int32Array,
    ArrowInt32Growable,
    arrow2::array::growable::GrowablePrimitive<i32>
);
impl_primitive_growable_array!(
    Int64Array,
    ArrowInt64Growable,
    arrow2::array::growable::GrowablePrimitive<i64>
);
impl_primitive_growable_array!(
    Int128Array,
    ArrowInt128Growable,
    arrow2::array::growable::GrowablePrimitive<i128>
);
impl_primitive_growable_array!(
    UInt8Array,
    ArrowUInt8Growable,
    arrow2::array::growable::GrowablePrimitive<u8>
);
impl_primitive_growable_array!(
    UInt16Array,
    ArrowUInt16Growable,
    arrow2::array::growable::GrowablePrimitive<u16>
);
impl_primitive_growable_array!(
    UInt32Array,
    ArrowUInt32Growable,
    arrow2::array::growable::GrowablePrimitive<u32>
);
impl_primitive_growable_array!(
    UInt64Array,
    ArrowUInt64Growable,
    arrow2::array::growable::GrowablePrimitive<u64>
);
impl_primitive_growable_array!(
    Float32Array,
    ArrowFloat32Growable,
    arrow2::array::growable::GrowablePrimitive<f32>
);
impl_primitive_growable_array!(
    Float64Array,
    ArrowFloat64Growable,
    arrow2::array::growable::GrowablePrimitive<f64>
);
impl_primitive_growable_array!(
    BinaryArray,
    ArrowBinaryGrowable,
    arrow2::array::growable::GrowableBinary<i64>
);
impl_primitive_growable_array!(
    Utf8Array,
    ArrowUtf8Growable,
    arrow2::array::growable::GrowableUtf8<i64>
);
impl_primitive_growable_array!(
    ListArray,
    ArrowListGrowable,
    arrow2::array::growable::GrowableList<i64>
);
impl_primitive_growable_array!(
    FixedSizeListArray,
    ArrowFixedSizeListGrowable,
    arrow2::array::growable::GrowableFixedSizeList
);
impl_primitive_growable_array!(
    StructArray,
    ArrowStructGrowable,
    arrow2::array::growable::GrowableStruct
);

impl_logical_growable_array!(TimestampType);
impl_logical_growable_array!(DurationType);
impl_logical_growable_array!(DateType);
impl_logical_growable_array!(EmbeddingType);
impl_logical_growable_array!(FixedShapeImageType);
impl_logical_growable_array!(FixedShapeTensorType);
impl_logical_growable_array!(ImageType);
impl_logical_growable_array!(TensorType);
impl_logical_growable_array!(Decimal128Type);
