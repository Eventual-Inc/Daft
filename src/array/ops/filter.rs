use crate::{
    array::DataArray,
    datatypes::{
        logical::DateArray, BinaryArray, BooleanArray, DaftNumericType, ExtensionArray,
        FixedSizeListArray, ListArray, NullArray, StructArray, Utf8Array,
    },
    error::DaftResult,
};

use super::as_arrow::AsArrow;

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.as_arrow(), mask.as_arrow())?;
        Self::try_from((self.field.clone(), result))
    }
}

impl Utf8Array {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.as_arrow(), mask.as_arrow())?;
        Self::try_from((self.field.clone(), result))
    }
}

impl BinaryArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.as_arrow(), mask.as_arrow())?;
        Self::try_from((self.field.clone(), result))
    }
}

impl BooleanArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.as_arrow(), mask.as_arrow())?;
        Self::try_from((self.field.clone(), result))
    }
}

impl NullArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let set_bits = mask.len() - mask.as_arrow().values().unset_bits();
        Ok(NullArray::full_null(
            self.name(),
            self.data_type(),
            set_bits,
        ))
    }
}

impl ListArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.as_arrow(), mask.as_arrow())?;
        Self::try_from((self.field.clone(), result))
    }
}

impl FixedSizeListArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.as_arrow(), mask.as_arrow())?;
        Self::try_from((self.field.clone(), result))
    }
}

impl StructArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.as_arrow(), mask.as_arrow())?;
        Self::try_from((self.field.clone(), result))
    }
}

impl ExtensionArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.data(), mask.as_arrow())?;
        DataArray::try_from((self.field.clone(), result))
    }
}

#[cfg(feature = "python")]
impl crate::datatypes::PythonArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        use arrow2::array::Array;
        use pyo3::PyObject;

        use crate::array::pseudo_arrow::PseudoArrowArray;
        use crate::datatypes::PythonType;

        let mask = mask.as_arrow();

        // Apply the filter mask to the data values, regardless of validity.
        let new_values = {
            mask.iter()
                .map(|x| x.unwrap_or(false))
                .zip(self.as_arrow().values().iter())
                .filter_map(|(f, item)| if f { Some(item.clone()) } else { None })
                .collect::<Vec<PyObject>>()
        };

        // Apply the filter mask to the validity bitmap.
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
                    arrow2::compute::filter::filter(old_validity_array, mask)
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

impl DateArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let new_array = self.physical.filter(mask)?;
        Ok(Self::new(self.field.clone(), new_array))
    }
}
