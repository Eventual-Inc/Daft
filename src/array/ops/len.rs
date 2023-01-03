use crate::{
    array::data_array::{BaseArray, DataArray},
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
