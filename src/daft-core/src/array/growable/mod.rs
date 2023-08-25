use common_error::DaftResult;

use crate::{
    datatypes::{
        logical::LogicalArray, nested_arrays::FixedSizeListArray, BinaryArray, BooleanArray,
        DateType, Decimal128Type, DurationType, EmbeddingType, ExtensionArray, FixedShapeImageType,
        FixedShapeTensorType, Float32Array, Float64Array, ImageType, Int128Array, Int16Array,
        Int32Array, Int64Array, Int8Array, ListArray, NullArray, StructArray, TensorType,
        TimestampType, UInt16Array, UInt32Array, UInt64Array, UInt8Array, Utf8Array,
    },
    DataType, Series,
};

mod arrow_growable;
mod logical_growable;
mod nested_growable;

#[cfg(feature = "python")]
mod python_growable;
#[cfg(feature = "python")]
use crate::datatypes::PythonArray;

/// Describes a struct that can be extended from slices of other pre-existing Series.
/// This is very useful for abstracting many "physical" operations such as takes, broadcasts,
/// filters and more.
pub trait Growable<'a> {
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
    type GrowableType: Growable<'a>;

    fn make_growable(
        name: String,
        dtype: &DataType,
        arrays: Vec<&'a Self>,
        use_validity: bool,
        capacity: usize,
    ) -> Self::GrowableType;
}

impl<'a> GrowableArray<'a> for NullArray {
    type GrowableType = arrow_growable::ArrowNullGrowable<'a>;

    fn make_growable(
        name: String,
        dtype: &DataType,
        _arrays: Vec<&Self>,
        _use_validity: bool,
        _capacity: usize,
    ) -> Self::GrowableType {
        Self::GrowableType::new(name, dtype)
    }
}

#[cfg(feature = "python")]
impl<'a> GrowableArray<'a> for PythonArray {
    type GrowableType = python_growable::PythonGrowable<'a>;

    fn make_growable(
        name: String,
        dtype: &DataType,
        arrays: Vec<&'a Self>,
        _use_validity: bool,
        capacity: usize,
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
        use_validity: bool,
        capacity: usize,
    ) -> Self::GrowableType {
        arrow_growable::ArrowExtensionGrowable::new(name, dtype, arrays, use_validity, capacity)
    }
}

impl<'a> GrowableArray<'a> for FixedSizeListArray {
    type GrowableType = nested_growable::FixedSizeListGrowable<'a>;

    fn make_growable(
        name: String,
        dtype: &DataType,
        arrays: Vec<&'a Self>,
        use_validity: bool,
        capacity: usize,
    ) -> Self::GrowableType {
        nested_growable::FixedSizeListGrowable::new(name, dtype, arrays, use_validity, capacity)
    }
}

macro_rules! impl_arrow_growable_array {
    (
        $daft_array:ident,
        $growable:ty
    ) => {
        impl<'a> GrowableArray<'a> for $daft_array {
            type GrowableType = $growable;

            fn make_growable(
                name: String,
                dtype: &DataType,
                arrays: Vec<&'a Self>,
                use_validity: bool,
                capacity: usize,
            ) -> Self::GrowableType {
                <$growable>::new(name, dtype, arrays, use_validity, capacity)
            }
        }
    };
}

macro_rules! impl_logical_growable_array {
    (
        $daft_logical_type:ident, $growable_type:ty
    ) => {
        impl<'a> GrowableArray<'a> for LogicalArray<$daft_logical_type> {
            type GrowableType = $growable_type;

            fn make_growable(
                name: String,
                dtype: &DataType,
                arrays: Vec<&'a Self>,
                use_validity: bool,
                capacity: usize,
            ) -> Self::GrowableType {
                Self::GrowableType::new(name.clone(), dtype, arrays, use_validity, capacity)
            }
        }
    };
}

impl_arrow_growable_array!(BooleanArray, arrow_growable::ArrowBooleanGrowable<'a>);
impl_arrow_growable_array!(Int8Array, arrow_growable::ArrowInt8Growable<'a>);
impl_arrow_growable_array!(Int16Array, arrow_growable::ArrowInt16Growable<'a>);
impl_arrow_growable_array!(Int32Array, arrow_growable::ArrowInt32Growable<'a>);
impl_arrow_growable_array!(Int64Array, arrow_growable::ArrowInt64Growable<'a>);
impl_arrow_growable_array!(Int128Array, arrow_growable::ArrowInt128Growable<'a>);
impl_arrow_growable_array!(UInt8Array, arrow_growable::ArrowUInt8Growable<'a>);
impl_arrow_growable_array!(UInt16Array, arrow_growable::ArrowUInt16Growable<'a>);
impl_arrow_growable_array!(UInt32Array, arrow_growable::ArrowUInt32Growable<'a>);
impl_arrow_growable_array!(UInt64Array, arrow_growable::ArrowUInt64Growable<'a>);
impl_arrow_growable_array!(Float32Array, arrow_growable::ArrowFloat32Growable<'a>);
impl_arrow_growable_array!(Float64Array, arrow_growable::ArrowFloat64Growable<'a>);
impl_arrow_growable_array!(BinaryArray, arrow_growable::ArrowBinaryGrowable<'a>);
impl_arrow_growable_array!(Utf8Array, arrow_growable::ArrowUtf8Growable<'a>);
impl_arrow_growable_array!(ListArray, arrow_growable::ArrowListGrowable<'a>);
impl_arrow_growable_array!(StructArray, arrow_growable::ArrowStructGrowable<'a>);

impl_logical_growable_array!(
    TimestampType,
    logical_growable::LogicalTimestampGrowable<'a>
);
impl_logical_growable_array!(DurationType, logical_growable::LogicalDurationGrowable<'a>);
impl_logical_growable_array!(DateType, logical_growable::LogicalDateGrowable<'a>);
impl_logical_growable_array!(
    EmbeddingType,
    logical_growable::LogicalEmbeddingGrowable<'a>
);
impl_logical_growable_array!(
    FixedShapeImageType,
    logical_growable::LogicalFixedShapeImageGrowable<'a>
);
impl_logical_growable_array!(
    FixedShapeTensorType,
    logical_growable::LogicalFixedShapeTensorGrowable<'a>
);
impl_logical_growable_array!(ImageType, logical_growable::LogicalImageGrowable<'a>);
impl_logical_growable_array!(TensorType, logical_growable::LogicalTensorGrowable<'a>);
impl_logical_growable_array!(
    Decimal128Type,
    logical_growable::LogicalDecimal128Growable<'a>
);
