use arrow2::types::Index;
use common_error::DaftResult;

use super::as_arrow::AsArrow;
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
    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let result = arrow2::compute::take::take(self.data(), idx.as_arrow())?;
        Self::try_from((self.field.clone(), result))
    }
}

// Default implementations of take op for DataArray and LogicalArray.
macro_rules! impl_dataarray_take {
    ($ArrayT:ty) => {
        impl $ArrayT {
            pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
            where
                I: DaftIntegerType,
                <I as DaftNumericType>::Native: arrow2::types::Index,
            {
                let result = arrow2::compute::take::take(self.data(), idx.as_arrow())?;
                Self::try_from((self.field.clone(), result))
            }
        }
    };
}

macro_rules! impl_logicalarray_take {
    ($ArrayT:ty) => {
        impl $ArrayT {
            pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
            where
                I: DaftIntegerType,
                <I as DaftNumericType>::Native: arrow2::types::Index,
            {
                let new_array = self.physical.take(idx)?;
                Ok(Self::new(self.field.clone(), new_array))
            }
        }
    };
}

impl_dataarray_take!(Utf8Array);
impl_dataarray_take!(BooleanArray);
impl_dataarray_take!(BinaryArray);
impl_dataarray_take!(NullArray);
impl_dataarray_take!(ExtensionArray);
impl_dataarray_take!(IntervalArray);
impl_dataarray_take!(Decimal128Array);

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
impl<T> FileArray<T>
where
    T: DaftMediaType,
{
    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let new_array = self.physical.take(idx)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}

impl FixedSizeBinaryArray {
    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
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

#[cfg(feature = "python")]
impl PythonArray {
    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
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

impl FixedSizeListArray {
    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
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

impl ListArray {
    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let child_capacity = idx
            .as_arrow()
            .iter()
            .map(|idx| match idx {
                None => 0,
                Some(idx) => {
                    let (start, end) = self.offsets().start_end(idx.to_usize());
                    end - start
                }
            })
            .sum();
        let mut growable = <Self as GrowableArray>::GrowableType::new(
            self.name(),
            self.data_type(),
            vec![self],
            idx.data().null_count() > 0,
            idx.len(),
            child_capacity,
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

impl StructArray {
    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let idx_as_u64 = idx.cast(&DataType::UInt64)?;
        let taken_validity = self.validity().map(|v| {
            arrow2::bitmap::Bitmap::from_iter(idx.into_iter().map(|i| match i {
                None => false,
                Some(i) => v.get_bit(i.to_usize()),
            }))
        });
        Ok(Self::new(
            self.field.clone(),
            self.children
                .iter()
                .map(|c| c.take(&idx_as_u64))
                .collect::<DaftResult<Vec<_>>>()?,
            taken_validity,
        ))
    }
}
