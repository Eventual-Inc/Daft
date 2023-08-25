use common_error::DaftResult;

use crate::{
    datatypes::{
        logical::{
            DateArray, Decimal128Array, DurationArray, EmbeddingArray, FixedShapeImageArray,
            FixedShapeTensorArray, ImageArray, TensorArray, TimestampArray,
        },
        BinaryArray, BooleanArray, ExtensionArray, FixedSizeListArray, Float32Array, Float64Array,
        Int128Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray, NullArray,
        StructArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array, Utf8Array,
    },
    DataType, Series,
};

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
pub trait GrowableArray {
    type GrowableType<'a>: Growable
    where
        Self: 'a;

    fn make_growable<'a>(
        name: String,
        dtype: &DataType,
        arrays: Vec<&'a Self>,
        use_validity: bool,
        capacity: usize,
    ) -> Self::GrowableType<'a>;
}

impl GrowableArray for NullArray {
    type GrowableType<'a> = arrow_growable::ArrowNullGrowable<'a>;

    fn make_growable<'a>(
        name: String,
        dtype: &DataType,
        _arrays: Vec<&'a Self>,
        _use_validity: bool,
        _capacity: usize,
    ) -> Self::GrowableType<'a> {
        Self::GrowableType::new(name, dtype)
    }
}

#[cfg(feature = "python")]
impl GrowableArray for PythonArray {
    type GrowableType<'a> = python_growable::PythonGrowable<'a>;

    fn make_growable<'a>(
        name: String,
        dtype: &DataType,
        arrays: Vec<&'a Self>,
        _use_validity: bool,
        capacity: usize,
    ) -> Self::GrowableType<'a> {
        python_growable::PythonGrowable::new(name, dtype, arrays, capacity)
    }
}

macro_rules! impl_growable_array {
    (
        $daft_array:ident,
        $growable:ty
    ) => {
        impl GrowableArray for $daft_array {
            type GrowableType<'a> = $growable;

            fn make_growable<'a>(
                name: String,
                dtype: &DataType,
                arrays: Vec<&'a Self>,
                use_validity: bool,
                capacity: usize,
            ) -> Self::GrowableType<'a> {
                Self::GrowableType::new(name, dtype, arrays, use_validity, capacity)
            }
        }
    };
}

impl_growable_array!(BooleanArray, arrow_growable::ArrowBooleanGrowable<'a>);
impl_growable_array!(Int8Array, arrow_growable::ArrowInt8Growable<'a>);
impl_growable_array!(Int16Array, arrow_growable::ArrowInt16Growable<'a>);
impl_growable_array!(Int32Array, arrow_growable::ArrowInt32Growable<'a>);
impl_growable_array!(Int64Array, arrow_growable::ArrowInt64Growable<'a>);
impl_growable_array!(Int128Array, arrow_growable::ArrowInt128Growable<'a>);
impl_growable_array!(UInt8Array, arrow_growable::ArrowUInt8Growable<'a>);
impl_growable_array!(UInt16Array, arrow_growable::ArrowUInt16Growable<'a>);
impl_growable_array!(UInt32Array, arrow_growable::ArrowUInt32Growable<'a>);
impl_growable_array!(UInt64Array, arrow_growable::ArrowUInt64Growable<'a>);
impl_growable_array!(Float32Array, arrow_growable::ArrowFloat32Growable<'a>);
impl_growable_array!(Float64Array, arrow_growable::ArrowFloat64Growable<'a>);
impl_growable_array!(BinaryArray, arrow_growable::ArrowBinaryGrowable<'a>);
impl_growable_array!(Utf8Array, arrow_growable::ArrowUtf8Growable<'a>);
impl_growable_array!(ListArray, arrow_growable::ArrowListGrowable<'a>);
impl_growable_array!(
    FixedSizeListArray,
    arrow_growable::ArrowFixedSizeListGrowable<'a>
);
impl_growable_array!(StructArray, arrow_growable::ArrowStructGrowable<'a>);
impl_growable_array!(ExtensionArray, arrow_growable::ArrowExtensionGrowable<'a>);
impl_growable_array!(
    TimestampArray,
    logical_growable::LogicalTimestampGrowable<'a>
);
impl_growable_array!(DurationArray, logical_growable::LogicalDurationGrowable<'a>);
impl_growable_array!(DateArray, logical_growable::LogicalDateGrowable<'a>);
impl_growable_array!(
    EmbeddingArray,
    logical_growable::LogicalEmbeddingGrowable<'a>
);
impl_growable_array!(
    FixedShapeImageArray,
    logical_growable::LogicalFixedShapeImageGrowable<'a>
);
impl_growable_array!(
    FixedShapeTensorArray,
    logical_growable::LogicalFixedShapeTensorGrowable<'a>
);
impl_growable_array!(ImageArray, logical_growable::LogicalImageGrowable<'a>);
impl_growable_array!(TensorArray, logical_growable::LogicalTensorGrowable<'a>);
impl_growable_array!(
    Decimal128Array,
    logical_growable::LogicalDecimal128Growable<'a>
);
