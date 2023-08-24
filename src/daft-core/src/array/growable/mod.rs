use common_error::DaftResult;

use crate::{
    datatypes::{
        logical::LogicalArray, BinaryArray, BooleanArray, DaftLogicalType, DateType,
        Decimal128Type, DurationType, EmbeddingType, ExtensionArray, FixedShapeImageType,
        FixedShapeTensorType, FixedSizeListArray, Float32Array, Float64Array, ImageType,
        Int128Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray, NullArray,
        StructArray, TensorType, TimestampType, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
        Utf8Array,
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

/// Trait that an Array type can implement to provide a Growable factory method
pub trait GrowableArray<'a>
where
    Self: Sized,
{
    type GrowableType: Growable;

    fn make_growable(
        name: String,
        dtype: &DataType,
        arrays: Vec<&'a Self>,
        capacity: usize,
        use_validity: bool,
    ) -> Self::GrowableType;
}

impl<'a> GrowableArray<'a> for NullArray {
    type GrowableType = arrow_growable::ArrowNullGrowable<'a>;

    fn make_growable(
        name: String,
        dtype: &DataType,
        _arrays: Vec<&Self>,
        _capacity: usize,
        _use_validity: bool,
    ) -> Self::GrowableType {
        arrow_growable::ArrowNullGrowable::new(
            name,
            dtype,
            arrow2::array::growable::GrowableNull::new(dtype.to_arrow().unwrap()),
        )
    }
}

#[cfg(feature = "python")]
impl<'a> GrowableArray<'a> for PythonArray {
    type GrowableType = python_growable::PythonGrowable<'a>;

    fn make_growable(
        name: String,
        dtype: &DataType,
        arrays: Vec<&'a Self>,
        capacity: usize,
        _use_validity: bool,
    ) -> Self::GrowableType {
        python_growable::PythonGrowable::new(name, dtype, arrays, capacity)
    }
}

impl<'a> GrowableArray<'a> for ExtensionArray {
    type GrowableType = arrow_growable::ArrowExtensionGrowable<'a>;

    fn make_growable(
        name: String,
        dtype: &DataType,
        arrays: Vec<&'a Self>,
        capacity: usize,
        use_validity: bool,
    ) -> Self::GrowableType {
        let arrow_arrays = arrays.iter().map(|arr| arr.data()).collect::<Vec<_>>();
        let arrow2_growable =
            arrow2::array::growable::make_growable(arrow_arrays.as_slice(), use_validity, capacity);
        arrow_growable::ArrowExtensionGrowable::new(name, dtype, arrow2_growable)
    }
}

macro_rules! impl_primitive_growable_array {
    (
        $daft_array:ident,
        $growable:ty,
        $arrow_growable:ty
    ) => {
        impl<'a> GrowableArray<'a> for $daft_array {
            type GrowableType = $growable;

            fn make_growable(
                name: String,
                dtype: &DataType,
                arrays: Vec<&'a Self>,
                capacity: usize,
                use_validity: bool,
            ) -> Self::GrowableType {
                <$growable>::new(
                    name,
                    dtype,
                    <$arrow_growable>::new(
                        arrays.iter().map(|a| a.as_arrow()).collect::<Vec<_>>(),
                        use_validity,
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
        impl<'a> GrowableArray<'a> for LogicalArray<$daft_logical_type> {
            type GrowableType = logical_growable::LogicalGrowable<'a, $daft_logical_type>;

            fn make_growable(
                name: String,
                dtype: &DataType,
                arrays: Vec<&'a Self>,
                capacity: usize,
                use_validity: bool,
            ) -> Self::GrowableType {
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
                        use_validity,
                    )),
                )
            }
        }
    };
}

impl_primitive_growable_array!(
    BooleanArray,
    arrow_growable::ArrowBooleanGrowable<'a>,
    arrow2::array::growable::GrowableBoolean
);
impl_primitive_growable_array!(
    Int8Array,
    arrow_growable::ArrowInt8Growable<'a>,
    arrow2::array::growable::GrowablePrimitive<i8>
);
impl_primitive_growable_array!(
    Int16Array,
    arrow_growable::ArrowInt16Growable<'a>,
    arrow2::array::growable::GrowablePrimitive<i16>
);
impl_primitive_growable_array!(
    Int32Array,
    arrow_growable::ArrowInt32Growable<'a>,
    arrow2::array::growable::GrowablePrimitive<i32>
);
impl_primitive_growable_array!(
    Int64Array,
    arrow_growable::ArrowInt64Growable<'a>,
    arrow2::array::growable::GrowablePrimitive<i64>
);
impl_primitive_growable_array!(
    Int128Array,
    arrow_growable::ArrowInt128Growable<'a>,
    arrow2::array::growable::GrowablePrimitive<i128>
);
impl_primitive_growable_array!(
    UInt8Array,
    arrow_growable::ArrowUInt8Growable<'a>,
    arrow2::array::growable::GrowablePrimitive<u8>
);
impl_primitive_growable_array!(
    UInt16Array,
    arrow_growable::ArrowUInt16Growable<'a>,
    arrow2::array::growable::GrowablePrimitive<u16>
);
impl_primitive_growable_array!(
    UInt32Array,
    arrow_growable::ArrowUInt32Growable<'a>,
    arrow2::array::growable::GrowablePrimitive<u32>
);
impl_primitive_growable_array!(
    UInt64Array,
    arrow_growable::ArrowUInt64Growable<'a>,
    arrow2::array::growable::GrowablePrimitive<u64>
);
impl_primitive_growable_array!(
    Float32Array,
    arrow_growable::ArrowFloat32Growable<'a>,
    arrow2::array::growable::GrowablePrimitive<f32>
);
impl_primitive_growable_array!(
    Float64Array,
    arrow_growable::ArrowFloat64Growable<'a>,
    arrow2::array::growable::GrowablePrimitive<f64>
);
impl_primitive_growable_array!(
    BinaryArray,
    arrow_growable::ArrowBinaryGrowable<'a>,
    arrow2::array::growable::GrowableBinary<i64>
);
impl_primitive_growable_array!(
    Utf8Array,
    arrow_growable::ArrowUtf8Growable<'a>,
    arrow2::array::growable::GrowableUtf8<i64>
);
impl_primitive_growable_array!(
    ListArray,
    arrow_growable::ArrowListGrowable<'a>,
    arrow2::array::growable::GrowableList<i64>
);
impl_primitive_growable_array!(
    FixedSizeListArray,
    arrow_growable::ArrowFixedSizeListGrowable<'a>,
    arrow2::array::growable::GrowableFixedSizeList
);
impl_primitive_growable_array!(
    StructArray,
    arrow_growable::ArrowStructGrowable<'a>,
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
