use std::iter::repeat;

use crate::{
    array::{DataArray, FixedSizeListArray, StructArray},
    datatypes::{
        logical::{
            DateArray, Decimal128Array, DurationArray, EmbeddingArray, FixedShapeImageArray,
            FixedShapeTensorArray, ImageArray, TensorArray, TimestampArray,
        },
        BinaryArray, BooleanArray, DaftIntegerType, DaftNumericType, ExtensionArray, ListArray,
        NullArray, UInt64Array, Utf8Array,
    },
    DataType, IntoSeries,
};
use common_error::DaftResult;
use num_traits::ToPrimitive;

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
impl_dataarray_take!(NullArray);
impl_dataarray_take!(ExtensionArray);
impl_logicalarray_take!(Decimal128Array);
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
        let size = self.fixed_element_len() as u64;
        let idx_as_u64 = idx.cast(&DataType::UInt64)?;
        let expanded_child_idx: Vec<Option<u64>> = idx_as_u64
            .u64()?
            .into_iter()
            .flat_map(|i| {
                let x: Box<dyn Iterator<Item = Option<u64>>> = match &i {
                    None => Box::new(repeat(None).take(size as usize)),
                    Some(i) => Box::new((*i * size..(*i + 1) * size).map(Some)),
                };
                x
            })
            .collect();
        let child_idx = UInt64Array::from((
            "",
            Box::new(arrow2::array::UInt64Array::from_iter(
                expanded_child_idx.iter(),
            )),
        ))
        .into_series();
        let taken_validity = self.validity.as_ref().map(|v| {
            arrow2::bitmap::Bitmap::from_iter(idx.into_iter().map(|i| match i {
                None => false,
                Some(i) => v.get_bit(i.to_usize().unwrap()),
            }))
        });
        Ok(Self::new(
            self.field.clone(),
            self.flat_child.take(&child_idx)?,
            taken_validity,
        ))
    }
}

impl StructArray {
    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let idx_as_u64 = idx.cast(&DataType::UInt64)?;
        let taken_validity = self.validity.as_ref().map(|v| {
            arrow2::bitmap::Bitmap::from_iter(idx.into_iter().map(|i| match i {
                None => false,
                Some(i) => v.get_bit(i.to_usize().unwrap()),
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

impl TensorArray {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<Box<dyn arrow2::array::Array>> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        let arrow_array = self.as_arrow();
        let is_valid = arrow_array
            .validity()
            .map_or(true, |validity| validity.get_bit(idx));
        if is_valid {
            let data_array = arrow_array.values()[0]
                .as_any()
                .downcast_ref::<arrow2::array::ListArray<i64>>()?;
            Some(unsafe { data_array.value_unchecked(idx) })
        } else {
            None
        }
    }

    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let new_array = self.physical.take(idx)?;
        Ok(Self::new(self.field.clone(), new_array))
    }

    pub fn str_value(&self, idx: usize) -> DaftResult<String> {
        let val = self.get(idx);
        match val {
            None => Ok("None".to_string()),
            Some(v) => Ok(format!("{v:?}")),
        }
    }

    pub fn html_value(&self, idx: usize) -> String {
        let str_value = self.str_value(idx).unwrap();
        html_escape::encode_text(&str_value)
            .into_owned()
            .replace('\n', "<br />")
    }
}

impl FixedShapeTensorArray {
    pub fn take<I>(&self, idx: &DataArray<I>) -> DaftResult<Self>
    where
        I: DaftIntegerType,
        <I as DaftNumericType>::Native: arrow2::types::Index,
    {
        let new_array = self.physical.take(idx)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}
