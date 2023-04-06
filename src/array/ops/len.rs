use arrow2::bitmap::Bitmap;

use crate::{
    array::{BaseArray, DataArray},
    datatypes::{BinaryArray, BooleanArray, DaftDataType, DaftNumericType, NullArray, Utf8Array},
};

impl<T> DataArray<T>
where
    T: DaftDataType + 'static,
{
    pub fn len(&self) -> usize {
        self.data().len()
    }
}

fn validity_byte_size(bitmask: Option<&Bitmap>) -> usize {
    match bitmask {
        None => 0,
        Some(b) => (b.len() + 8 - 1) / 8,
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
        numerical_data_size + validity_byte_size(self.data().validity())
    }
}

impl BooleanArray {
    pub fn size_bytes(&self) -> usize {
        let bitmask_size = (self.len() + 8 - 1) / 8;
        bitmask_size + validity_byte_size(self.data().validity())
    }
}

impl Utf8Array {
    pub fn size_bytes(&self) -> usize {
        use core::mem::size_of;

        let arrow_array = self.downcast();
        let buffer_size = (arrow_array.offsets().last().unwrap()
            - arrow_array.offsets().first().unwrap()) as usize;
        let offset_size = self.len() * size_of::<i64>();

        buffer_size + offset_size + validity_byte_size(self.data().validity())
    }
}

impl BinaryArray {
    pub fn size_bytes(&self) -> usize {
        use core::mem::size_of;

        let arrow_array = self.downcast();
        let buffer_size = (arrow_array.offsets().last().unwrap()
            - arrow_array.offsets().first().unwrap()) as usize;
        let offset_size = self.len() * size_of::<i64>();

        buffer_size + offset_size + validity_byte_size(self.data().validity())
    }
}

impl NullArray {
    pub fn size_bytes(&self) -> usize {
        0
    }
}
