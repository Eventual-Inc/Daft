use arrow2::array::Array;

use crate::{
    array::DataArray,
    datatypes::{
        BinaryArray, BooleanArray, DaftNumericType, FixedSizeListArray, ListArray, NullArray,
        Utf8Array,
    },
    error::DaftResult,
};

use crate::array::BaseArray;

use super::downcast::Downcastable;

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.downcast(), mask.downcast())?;
        DataArray::try_from((self.name(), result))
    }
}

impl Utf8Array {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.downcast(), mask.downcast())?;
        DataArray::try_from((self.name(), result))
    }
}

impl BinaryArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.downcast(), mask.downcast())?;
        DataArray::try_from((self.name(), result))
    }
}

impl BooleanArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.downcast(), mask.downcast())?;
        DataArray::try_from((self.name(), result))
    }
}

impl NullArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let set_bits = mask.len() - mask.downcast().values().unset_bits();
        Ok(NullArray::full_null(self.name(), set_bits))
    }
}

impl ListArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.downcast(), mask.downcast())?;
        DataArray::try_from((self.name(), result))
    }
}

impl FixedSizeListArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        let result = arrow2::compute::filter::filter(self.downcast(), mask.downcast())?;
        DataArray::try_from((self.name(), result))
    }
}

#[cfg(feature = "python")]
impl crate::datatypes::PythonArray {
    pub fn filter(&self, mask: &BooleanArray) -> DaftResult<Self> {
        use crate::array::vec_backed::VecBackedArray;
        use crate::datatypes::PythonType;
        use pyo3::PyObject;

        let mask = mask.downcast();
        let mask_iter = if mask.null_count() > 0 {
            unimplemented!()
        } else {
            mask.values().iter()
        };

        let values_vec = {
            let self_values = self.downcast().vec().iter();
            mask_iter
                .zip(self_values)
                .filter_map(|(f, item)| if f { Some(item.clone()) } else { None })
                .collect::<Vec<PyObject>>()
        };

        let arrow_array: Box<dyn arrow2::array::Array> = Box::new(VecBackedArray::new(values_vec));

        DataArray::<PythonType>::new(self.field().clone().into(), arrow_array)
    }
}
