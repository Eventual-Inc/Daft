use common_error::DaftResult;
use daft_arrow::types::Index;

use crate::{
    array::{
        growable::{Growable, GrowableArray},
        prelude::*,
    },
    datatypes::{FileArray, IntervalArray, prelude::*},
    file::DaftMediaType,
};

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let result = arrow::compute::take(self.to_arrow().as_ref(), idx.to_arrow().as_ref(), None)?;
        Self::from_arrow(self.field.clone(), result)
    }
}

// Default implementations of take op for DataArray and LogicalArray.
macro_rules! impl_infallible_take {
    ($ArrayT:ty) => {
        impl $ArrayT {
            pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
                let result =
                    arrow::compute::take(self.to_arrow().as_ref(), idx.to_arrow().as_ref(), None)?;
                Self::from_arrow(self.field.clone(), result)
            }
        }
    };
}

macro_rules! impl_fallible_take {
    ($ArrayT:ty) => {
        impl $ArrayT {
            pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
                let result =
                    arrow::compute::take(self.to_arrow()?.as_ref(), idx.to_arrow().as_ref(), None)?;
                Self::from_arrow(self.field.clone(), result)
            }
        }
    };
}

macro_rules! impl_logicalarray_take {
    ($ArrayT:ty) => {
        impl $ArrayT {
            pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
                let new_array = self.physical.take(idx)?;
                Ok(Self::new(self.field.clone(), new_array))
            }
        }
    };
}

impl_infallible_take!(Utf8Array);
impl_infallible_take!(BooleanArray);
impl_infallible_take!(BinaryArray);
impl_infallible_take!(NullArray);
impl_infallible_take!(ExtensionArray);
impl_infallible_take!(IntervalArray);
impl_infallible_take!(Decimal128Array);
impl_infallible_take!(FixedSizeBinaryArray);

impl_logicalarray_take!(DateArray);
impl_logicalarray_take!(TimeArray);
impl_logicalarray_take!(DurationArray);
impl_logicalarray_take!(TimestampArray);
impl_logicalarray_take!(EmbeddingArray);
impl_logicalarray_take!(ImageArray);
impl_logicalarray_take!(FixedShapeImageArray);
impl_logicalarray_take!(TensorArray);
impl_logicalarray_take!(SparseTensorArray);
impl_logicalarray_take!(FixedShapeSparseTensorArray);
impl_logicalarray_take!(FixedShapeTensorArray);
impl_logicalarray_take!(MapArray);

impl_fallible_take!(FixedSizeListArray);
impl_fallible_take!(ListArray);
impl_fallible_take!(StructArray);
impl<T> FileArray<T>
where
    T: DaftMediaType,
{
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let new_array = self.physical.take(idx)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}

#[cfg(feature = "python")]
impl PythonArray {
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let mut growable = Self::make_growable(
            self.name(),
            self.data_type(),
            vec![self],
            idx.data().null_count() > 0,
            idx.len(),
        );

        for i in idx {
            match i {
                None => {
                    growable.add_nulls(1);
                }
                Some(i) => {
                    growable.extend(0, i.to_usize(), 1);
                }
            }
        }

        Ok(growable.build()?.downcast::<Self>()?.clone())
    }
}
