use crate::{
    array::DataArray,
    datatypes::{DaftNumericType, Utf8Array},
};

impl<T> DataArray<T>
where
    T: DaftNumericType,
{
    #[inline]
    pub fn get(&self, idx: usize) -> Option<T::Native> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        let arrow_array = self.downcast();
        let is_valid = match arrow_array.validity() {
            Some(validity) => validity.get_bit(idx),
            None => true,
        };
        if is_valid {
            Some(unsafe { arrow_array.value_unchecked(idx) })
        } else {
            None
        }
    }
}

impl Utf8Array {
    #[inline]
    pub fn get(&self, idx: usize) -> Option<&str> {
        if idx >= self.len() {
            panic!("Out of bounds: {} vs len: {}", idx, self.len())
        }
        let arrow_array = self.downcast();
        let is_valid = match arrow_array.validity() {
            Some(validity) => validity.get_bit(idx),
            None => true,
        };
        if is_valid {
            Some(unsafe { arrow_array.value_unchecked(idx) })
        } else {
            None
        }
    }
}
