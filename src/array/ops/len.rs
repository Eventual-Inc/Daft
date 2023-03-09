use crate::{
    array::{BaseArray, DataArray},
    datatypes::{BooleanArray, DaftDataType, DaftNumericType, NullArray, Utf8Array},
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
    T: DaftNumericType,
{
    pub fn size_bytes(&self) -> usize {
        use core::mem::size_of;
        let bytes_per_elem = size_of::<T::Native>();
        let numerical_data_size = self.len() * bytes_per_elem;
        let bitmask_data_size = match self.data().validity() {
            None => 0,
            Some(_) => (self.len() + 8 - 1) / 8,
        };

        numerical_data_size + bitmask_data_size
    }
}

impl BooleanArray {
    pub fn size_bytes(&self) -> usize {
        (self.len() + 8 - 1) / 8
    }
}

impl Utf8Array {
    pub fn size_bytes(&self) -> usize {
        use core::mem::size_of;

        let arrow_array = self.downcast();
        let buffer_size = (arrow_array.offsets().last().unwrap()
            - arrow_array.offsets().first().unwrap()) as usize;
        let offset_size = (self.len() + 1) * size_of::<i64>();
        let bitmask_data_size = match self.data().validity() {
            None => 0,
            Some(_) => (self.len() + 8 - 1) / 8,
        };
        buffer_size + offset_size + bitmask_data_size
    }
}

impl NullArray {
    pub fn size_bytes(&self) -> usize {
        0
    }
}
