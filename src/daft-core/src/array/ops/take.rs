use std::sync::Arc;

use common_error::DaftResult;
use daft_arrow::{
    array::MutableFixedSizeBinaryArray,
    buffer::Buffer,
    types::Index,
};

use super::{as_arrow::AsArrow, from_arrow::FromArrow};
#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    array::prelude::*,
    datatypes::{FileArray, IntervalArray, prelude::*},
    file::DaftMediaType,
};

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let result = daft_arrow::compute::take::take(self.data(), idx.as_arrow())?;
        Self::try_from((self.field.clone(), result))
    }
}

// Default implementations of take op for DataArray and LogicalArray.
macro_rules! impl_dataarray_take {
    ($ArrayT:ty) => {
        impl $ArrayT {
            pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
                let result = daft_arrow::compute::take::take(self.data(), idx.as_arrow())?;
                Self::try_from((self.field.clone(), result))
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
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let new_array = self.physical.take(idx)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}

impl FixedSizeBinaryArray {
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let arrow_array = self.as_arrow();
        let size = arrow_array.size();
        let mut mutable = MutableFixedSizeBinaryArray::with_capacity(size, idx.len());

        for i in idx {
            match i {
                None => {
                    mutable.push(None::<&[u8]>);
                }
                Some(i) => {
                    let idx_usize = i.to_usize();
                    match arrow_array.get(idx_usize) {
                        Some(value) => {
                            let value: &[u8] = value;
                            mutable.push(Some(value));
                        }
                        None => {
                            mutable.push(None::<&[u8]>);
                        }
                    }
                }
            }
        }

        let arrow_result: daft_arrow::array::FixedSizeBinaryArray = mutable.into();
        Self::try_from((self.field.clone(), arrow_result.boxed()))
    }
}

#[cfg(feature = "python")]
impl PythonArray {
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        use pyo3::Python;

        let mut values = Vec::with_capacity(idx.len());
        let mut validity = if idx.data().null_count() > 0 || self.validity().is_some() {
            Some(daft_arrow::buffer::NullBufferBuilder::new(idx.len()))
        } else {
            None
        };

        Python::attach(|_py| {
            for i in idx {
                match i {
                    None => {
                        values.push(Arc::new(Python::attach(|py| py.None())));
                        if let Some(ref mut v) = validity {
                            v.append_null();
                        }
                    }
                    Some(i) => {
                        let idx_usize = i.to_usize();
                        if self.is_valid(idx_usize) {
                            values.push(self.values().get(idx_usize).unwrap().clone());
                            if let Some(ref mut v) = validity {
                                v.append_non_null();
                            }
                        } else {
                            values.push(Arc::new(Python::attach(|py| py.None())));
                            if let Some(ref mut v) = validity {
                                v.append_null();
                            }
                        }
                    }
                }
            }
        });

        Ok(Self::new(
            Arc::new(self.field().clone()),
            Buffer::from(values),
            validity.map(|mut v| v.finish()).flatten(),
        ))
    }
}

impl FixedSizeListArray {
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let arrow_arr = self.to_arrow();
        let result = daft_arrow::compute::take::take(arrow_arr.as_ref(), idx.as_arrow())?;
        Self::from_arrow(self.field().clone().into(), result)
    }
}

impl ListArray {
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let arrow_arr = self.to_arrow();
        let result = daft_arrow::compute::take::take(arrow_arr.as_ref(), idx.as_arrow())?;
        Self::from_arrow(self.field().clone().into(), result)
    }
}

impl StructArray {
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let taken_validity = self.validity().map(|v| {
            daft_arrow::buffer::NullBuffer::from_iter(idx.into_iter().map(|i| match i {
                None => false,
                Some(i) => v.is_valid(i.to_usize()),
            }))
        });
        Ok(Self::new(
            self.field.clone(),
            self.children
                .iter()
                .map(|c| c.take(idx))
                .collect::<DaftResult<Vec<_>>>()?,
            taken_validity,
        ))
    }
}
