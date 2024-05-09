use crate::{
    array::{
        growable::{Growable, GrowableArray},
        DataArray, FixedSizeListArray, ListArray, StructArray,
    },
    datatypes::{
        logical::{
            DateArray, Decimal128Array, DurationArray, EmbeddingArray, FixedShapeImageArray,
            FixedShapeTensorArray, ImageArray, MapArray, TensorArray, TimeArray, TimestampArray,
        },
        BinaryArray, BooleanArray, DaftIntegerType, DaftNumericType, ExtensionArray,
        FixedSizeBinaryArray, NullArray, Utf8Array,
    },
    DataType,
};
use arrow2::types::Index;
use common_error::DaftResult;

use super::as_arrow::AsArrow;

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
impl_logicalarray_take!(Decimal128Array);
impl_logicalarray_take!(DateArray);
impl_logicalarray_take!(TimeArray);
impl_logicalarray_take!(DurationArray);
impl_logicalarray_take!(TimestampArray);
impl_logicalarray_take!(EmbeddingArray);
impl_logicalarray_take!(ImageArray);
impl_logicalarray_take!(FixedShapeImageArray);
impl_logicalarray_take!(TensorArray);
impl_logicalarray_take!(FixedShapeTensorArray);
impl_logicalarray_take!(MapArray);

impl FixedSizeBinaryArray {
    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let mut growable = FixedSizeBinaryArray::make_growable(
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

        Ok(growable
            .build()?
            .downcast::<FixedSizeBinaryArray>()?
            .clone())
    }
}

#[cfg(feature = "python")]
impl crate::datatypes::PythonArray {
    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        use crate::array::pseudo_arrow::PseudoArrowArray;
        use crate::datatypes::PythonType;

        use arrow2::array::Array;
        use pyo3::prelude::*;

        let indices = idx.as_arrow();

        let old_values = self.as_arrow().values();

        // Execute take on the data values, ignoring validity.
        let new_values: Vec<PyObject> = {
            let py_none = Python::with_gil(|py: Python| py.None());

            indices
                .iter()
                .map(|maybe_idx| match maybe_idx {
                    Some(idx) => old_values[arrow2::types::Index::to_usize(idx)].clone(),
                    None => py_none.clone(),
                })
                .collect()
        };

        // Execute take on the validity bitmap using arrow2::compute.
        let new_validity = {
            self.as_arrow()
                .validity()
                .map(|old_validity| {
                    let old_validity_array = {
                        &arrow2::array::BooleanArray::new(
                            arrow2::datatypes::DataType::Boolean,
                            old_validity.clone(),
                            None,
                        )
                    };
                    arrow2::compute::take::take(old_validity_array, indices)
                })
                .transpose()?
                .map(|new_validity_dynarray| {
                    let new_validity_iter = new_validity_dynarray
                        .as_any()
                        .downcast_ref::<arrow2::array::BooleanArray>()
                        .unwrap()
                        .iter();
                    arrow2::bitmap::Bitmap::from_iter(
                        new_validity_iter.map(|valid| valid.unwrap_or(false)),
                    )
                })
        };

        let arrow_array: Box<dyn arrow2::array::Array> =
            Box::new(PseudoArrowArray::new(new_values.into(), new_validity));

        DataArray::<PythonType>::new(self.field().clone().into(), arrow_array)
    }
}

impl FixedSizeListArray {
    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let mut growable = FixedSizeListArray::make_growable(
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

        Ok(growable.build()?.downcast::<FixedSizeListArray>()?.clone())
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
        let mut growable = <ListArray as GrowableArray>::GrowableType::new(
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

        Ok(growable.build()?.downcast::<ListArray>()?.clone())
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
