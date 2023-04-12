use crate::{
    array::{BaseArray, DataArray},
    datatypes::DaftDataType,
};

impl<T> DataArray<T>
where
    T: DaftDataType + 'static,
{
    pub fn len(&self) -> usize {
        self.data().len()
    }
}

impl<T> DataArray<T>
where
    T: DaftDataType + 'static,
{
    pub fn size_bytes(&self) -> usize {
        arrow2::compute::aggregate::estimated_bytes_size(self.data())
    }
}
