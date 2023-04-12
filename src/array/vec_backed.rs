use arrow2::{array::Array, bitmap::Bitmap, datatypes::DataType};
use core::any::Any;
use std::marker::{Send, Sync};

#[derive(Clone)]
pub struct VecBackedArray<T> {
    values: Vec<T>,
    // Special handling for None is TODO.
    // For now, it's far simpler to treat them as any other Python object.
    // validity: Option<Bitmap>,
}

impl<T: Clone> VecBackedArray<T> {
    pub fn new(values: Vec<T>) -> Self {
        VecBackedArray { values }
    }
    pub fn vec(&self) -> &Vec<T> {
        &self.values
    }
    pub fn concatenate(arrays: Vec<&Self>) -> Self {
        let mut concatenated_values: Vec<T> = Vec::new();
        for array in arrays {
            concatenated_values.extend_from_slice(array.vec());
        }
        VecBackedArray::new(concatenated_values)
    }
}

impl<T: Send + Sync + Clone + 'static> Array for VecBackedArray<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn data_type(&self) -> &DataType {
        unimplemented!("VecBackedArray does not hold real Arrow DataTypes")
    }

    fn validity(&self) -> Option<&Bitmap> {
        None
    }

    fn null_count(&self) -> usize {
        unimplemented!()
    }

    fn is_null(&self, _i: usize) -> bool {
        unimplemented!()
    }

    fn is_valid(&self, i: usize) -> bool {
        !self.is_null(i)
    }

    fn slice(&mut self, offset: usize, length: usize) {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        self.values = self.values[offset..(offset + length)].to_vec();
    }

    fn sliced(&self, offset: usize, length: usize) -> Box<dyn Array> {
        let values = self.values[offset..(offset + length)].to_vec();
        Box::new(VecBackedArray { values })
    }

    unsafe fn sliced_unchecked(&self, offset: usize, length: usize) -> Box<dyn Array> {
        let values = self
            .values
            .get_unchecked(offset..(offset + length))
            .to_vec();
        Box::new(VecBackedArray { values })
    }

    fn with_validity(&self, _validity: Option<Bitmap>) -> Box<dyn Array> {
        unimplemented!()
    }

    fn to_boxed(&self) -> Box<dyn Array> {
        Box::new(VecBackedArray {
            values: self.values.clone(),
        })
    }
}
