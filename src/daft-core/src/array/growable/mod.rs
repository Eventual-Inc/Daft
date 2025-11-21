use common_error::DaftResult;

use crate::{
    array::{FixedSizeListArray, ListArray, StructArray, prelude::*},
    datatypes::{FileArray, prelude::*},
    file::DaftMediaType,
    series::Series,
    with_match_daft_types,
};

mod arrow_growable;
mod bitmap_growable;
mod fixed_size_list_growable;
mod list_growable;
mod logical_growable;
mod map_growable;
mod struct_growable;

#[cfg(feature = "python")]
mod python_growable;

/// This function creates a [`Box<dyn Growable>`] when provided with a [`DataType`] and a [`Vec<&Series>`].
///
/// This is most useful when running dynamic code that operates on [`Series`] rather than concrete Array types
///
/// If working in Array kernels, it may be more useful instead to use [`GrowableArray::make_growable`] which is
/// a trait that is implemented on each concrete Array type.
///
/// # Arguments
///
/// * `name` - Name of the [`Series`] to be returned after building the [`Growable`]
/// * `dtype` - [`DataType`] of the built [`Series`]. **NOTE: This must match the dtype of each Series**
/// * `series` - A vector of `&Series` references to the [`Series`] that this [`Growable`] copies data from
/// * `use_validity` - Whether or not the [`Growable`] will need to grow a validity mask. Setting this to `false` will
///   lead to performance gains, but give bad answers if you try to call [`Growable::add_nulls`].
/// * `capacity` - Helps pre-allocate memory to the [`Growable`] by providing a capacity up-front. Note that variable-length types
///   such as [`ListArray`] only understands this as the "top-level" capacity, but the capacity of nested children arrays cannot be specified
///   through this [`make_growable`] API. Instead, you may wish to instantiate and use the [`nested_growable::ListGrowable`] directly if
///   this is important to your use-case.
pub fn make_growable<'a>(
    name: &str,
    dtype: &DataType,
    series: Vec<&'a Series>,
    use_validity: bool,
    capacity: usize,
) -> Box<dyn Growable + 'a> {
    with_match_daft_types!(dtype, |$T| {
        Box::new(<<$T as DaftDataType>::ArrayType as GrowableArray>::make_growable(
            name,
            dtype,
            series.iter().map(|s| s.downcast::<<$T as DaftDataType>::ArrayType>().unwrap()).collect::<Vec<_>>(),
            use_validity,
            capacity,
        ))
    })
}

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

    /// Extends this [`Growable`] with null elements (same as add_nulls with arrow naming convention).
    fn extend_nulls(&mut self, len: usize) {
        self.add_nulls(len);
    }

    /// Builds an array from the [`Growable`]
    fn build(&mut self) -> DaftResult<Series>;
}

/// Trait that an Array type can implement to provide a Growable factory method
pub trait GrowableArray {
    type GrowableType<'a>: Growable
    where
        Self: 'a;

    fn make_growable<'a>(
        name: &str,
        dtype: &DataType,
        arrays: Vec<&'a Self>,
        use_validity: bool,
        capacity: usize,
    ) -> Self::GrowableType<'a>;
}

impl GrowableArray for NullArray {
    type GrowableType<'a> = arrow_growable::ArrowNullGrowable<'a>;

    fn make_growable<'a>(
        name: &str,
        dtype: &DataType,
        _arrays: Vec<&'a Self>,
        _use_validity: bool,
        _capacity: usize,
    ) -> Self::GrowableType<'a> {
        Self::GrowableType::new(name, dtype)
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
                name: &str,
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

impl GrowableArray for ListArray {
    type GrowableType<'a> = list_growable::ListGrowable<'a>;

    fn make_growable<'a>(
        name: &str,
        dtype: &DataType,
        arrays: Vec<&'a Self>,
        use_validity: bool,
        capacity: usize,
    ) -> Self::GrowableType<'a> {
        Self::GrowableType::new(
            name,
            dtype,
            arrays,
            use_validity,
            capacity,
            // NOTE: use ListGrowable::new directly if you wish to specify the child Series' capacity
            0,
        )
    }
}

impl_growable_array!(BooleanArray, arrow_growable::ArrowBooleanGrowable<'a>);
impl_growable_array!(Int8Array, arrow_growable::ArrowInt8Growable<'a>);
impl_growable_array!(Int16Array, arrow_growable::ArrowInt16Growable<'a>);
impl_growable_array!(Int32Array, arrow_growable::ArrowInt32Growable<'a>);
impl_growable_array!(Int64Array, arrow_growable::ArrowInt64Growable<'a>);
impl_growable_array!(Decimal128Array, arrow_growable::ArrowDecimal128Growable<'a>);
impl_growable_array!(UInt8Array, arrow_growable::ArrowUInt8Growable<'a>);
impl_growable_array!(UInt16Array, arrow_growable::ArrowUInt16Growable<'a>);
impl_growable_array!(UInt32Array, arrow_growable::ArrowUInt32Growable<'a>);
impl_growable_array!(UInt64Array, arrow_growable::ArrowUInt64Growable<'a>);
impl_growable_array!(Float32Array, arrow_growable::ArrowFloat32Growable<'a>);
impl_growable_array!(Float64Array, arrow_growable::ArrowFloat64Growable<'a>);
impl_growable_array!(BinaryArray, arrow_growable::ArrowBinaryGrowable<'a>);
impl_growable_array!(
    FixedSizeBinaryArray,
    arrow_growable::ArrowFixedSizeBinaryGrowable<'a>
);
impl_growable_array!(Utf8Array, arrow_growable::ArrowUtf8Growable<'a>);
impl_growable_array!(ExtensionArray, arrow_growable::ArrowExtensionGrowable<'a>);
impl_growable_array!(
    FixedSizeListArray,
    fixed_size_list_growable::FixedSizeListGrowable<'a>
);
impl_growable_array!(StructArray, struct_growable::StructGrowable<'a>);
impl_growable_array!(
    TimestampArray,
    logical_growable::LogicalTimestampGrowable<'a>
);
impl_growable_array!(DurationArray, logical_growable::LogicalDurationGrowable<'a>);

impl_growable_array!(
    IntervalArray,
    arrow_growable::ArrowMonthDayNanoIntervalGrowable<'a>
);

impl_growable_array!(DateArray, logical_growable::LogicalDateGrowable<'a>);
impl_growable_array!(TimeArray, logical_growable::LogicalTimeGrowable<'a>);
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
impl_growable_array!(
    SparseTensorArray,
    logical_growable::LogicalSparseTensorGrowable<'a>
);
impl_growable_array!(
    FixedShapeSparseTensorArray,
    logical_growable::LogicalFixedShapeSparseTensorGrowable<'a>
);
impl_growable_array!(ImageArray, logical_growable::LogicalImageGrowable<'a>);
impl_growable_array!(TensorArray, logical_growable::LogicalTensorGrowable<'a>);
impl_growable_array!(MapArray, map_growable::MapGrowable<'a>);
impl<T> GrowableArray for FileArray<T>
where
    T: DaftMediaType,
{
    type GrowableType<'a> = logical_growable::LogicalFileGrowable<'a, T>;
    fn make_growable<'a>(
        name: &str,
        dtype: &DataType,
        arrays: Vec<&'a Self>,
        use_validity: bool,
        capacity: usize,
    ) -> Self::GrowableType<'a> {
        Self::GrowableType::new(name, dtype, arrays, use_validity, capacity)
    }
}

#[cfg(feature = "python")]
impl_growable_array!(PythonArray, python_growable::PythonGrowable<'a>);
