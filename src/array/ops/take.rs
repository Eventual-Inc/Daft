use crate::{
    array::DataArray,
    datatypes::{
        logical::{
            DateArray, DurationArray, EmbeddingArray, FixedShapeImageArray, ImageArray,
            TimestampArray,
        },
        BinaryArray, BooleanArray, DaftIntegerType, DaftNumericType, ExtensionArray,
        FixedSizeListArray, ListArray, NullArray, StructArray, Utf8Array,
    },
    error::DaftResult,
};

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
impl_dataarray_take!(ListArray);
impl_dataarray_take!(FixedSizeListArray);
impl_dataarray_take!(NullArray);
impl_dataarray_take!(StructArray);
impl_dataarray_take!(ExtensionArray);
impl_logicalarray_take!(DateArray);
impl_logicalarray_take!(DurationArray);
impl_logicalarray_take!(TimestampArray);
impl_logicalarray_take!(EmbeddingArray);
impl_logicalarray_take!(ImageArray);
impl_logicalarray_take!(FixedShapeImageArray);

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
        use arrow2::types::Index;
        use pyo3::prelude::*;

        let indices = idx.as_arrow();

        let old_values = self.as_arrow().values();

        // Execute take on the data values, ignoring validity.
        let new_values: Vec<PyObject> = {
            let py_none = Python::with_gil(|py: Python| py.None());

            indices
                .iter()
                .map(|maybe_idx| match maybe_idx {
                    Some(idx) => old_values[idx.to_usize()].clone(),
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
