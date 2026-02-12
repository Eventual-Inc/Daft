use std::sync::Arc;

use arrow::{array::NullBufferBuilder, buffer::NullBuffer};
use common_error::DaftResult;
use daft_arrow::{buffer::Buffer, types::Index};

#[cfg(feature = "python")]
use crate::prelude::PythonArray;
use crate::{
    array::prelude::*,
    datatypes::{FileArray, prelude::*},
    file::DaftMediaType,
};

impl<T> DataArray<T>
where
    T: DaftPhysicalType,
{
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let result = arrow::compute::take(self.to_arrow().as_ref(), idx.to_arrow().as_ref(), None)?;
        Self::from_arrow(self.field.clone(), result)
    }
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

impl FixedSizeListArray {
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let fixed_size = self.fixed_element_len();
        let mut child_indices = Vec::with_capacity(idx.len() * fixed_size);
        let mut nulls_builder = NullBufferBuilder::new(idx.len());

        for i in idx {
            match i {
                None => {
                    nulls_builder.append_null();
                    child_indices.extend(std::iter::repeat_n(0, fixed_size as _));
                }
                Some(i) => {
                    let i = i.to_usize();
                    nulls_builder.append(self.is_valid(i));
                    let start: u64 = i as u64 * fixed_size as u64;
                    child_indices.extend(start..start + fixed_size as u64);
                }
            }
        }

        let child_idx = UInt64Array::from_vec("", child_indices);
        let new_child = self.flat_child.take(&child_idx)?;

        Ok(Self::new(
            self.field.clone(),
            new_child,
            nulls_builder.finish(),
        ))
    }
}

impl ListArray {
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let mut new_offsets = Vec::with_capacity(idx.len() + 1);
        new_offsets.push(0i64);

        let mut child_indices = Vec::new();
        let mut nulls_builder = NullBufferBuilder::new(idx.len());

        for i in idx {
            match i {
                None => {
                    nulls_builder.append_null();
                    new_offsets.push(*new_offsets.last().unwrap());
                }
                Some(i) => {
                    let (start, end) = self.offsets().start_end(i.to_usize());
                    child_indices.extend(start..end);
                    new_offsets.push(*new_offsets.last().unwrap() + (end - start) as i64);
                    nulls_builder.append(self.is_valid(i.to_usize()));
                }
            }
        }
        let nulls = nulls_builder.finish();

        let child_idx = UInt64Array::from_values("", child_indices.into_iter().map(|i| i as u64));
        let new_child = self.flat_child.take(&child_idx)?;

        Ok(Self::new(
            self.field.clone(),
            new_child,
            new_offsets.try_into()?,
            nulls,
        ))
    }
}
impl StructArray {
    pub fn take(&self, idx: &UInt64Array) -> DaftResult<Self> {
        let nulls = self.nulls().map(|v| {
            NullBuffer::from_iter(idx.into_iter().map(|i| match i {
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
            nulls,
        ))
    }
}
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
        use pyo3::Python;

        let mut values = Vec::with_capacity(idx.len());
        let mut validity = if idx.data().null_count() > 0 || self.nulls().is_some() {
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
            validity.and_then(|mut v| v.finish()),
        ))
    }
}
