use arrow2::{array::Array, bitmap::Bitmap, datatypes::DataType};
use core::any::Any;
use std::marker::{Send, Sync};

#[derive(Clone)]
pub struct VecBackedArray<T> {
    values: Vec<T>,
    validity: Option<Bitmap>,
}

impl<T> VecBackedArray<T> {
    pub fn new(values: Vec<T>, validity: Option<Bitmap>) -> Self {
        VecBackedArray { values, validity }
    }
    pub fn vec(&self) -> &Vec<T> {
        &self.values
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
        // You need to define the `DataType` for `VecBackedArray`.
        // For example, if `T` is `i32`, you can use:
        // &DataType::Int32
        unimplemented!()
    }

    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    fn null_count(&self) -> usize {
        self.validity()
            .as_ref()
            .map(|x| x.unset_bits())
            .unwrap_or(0)
    }

    fn is_null(&self, i: usize) -> bool {
        self.validity()
            .as_ref()
            .map(|x| !x.get_bit(i))
            .unwrap_or(false)
    }

    fn is_valid(&self, i: usize) -> bool {
        !self.is_null(i)
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        let values = self.values[offset..(offset + length)].to_vec();
        let validity = self.validity.clone();
        Box::new(VecBackedArray { values, validity })
    }

    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Box<dyn Array> {
        let values = self
            .values
            .get_unchecked(offset..(offset + length))
            .to_vec();
        let validity = self.validity.clone();
        Box::new(VecBackedArray { values, validity })
    }

    fn with_validity(&self, validity: Option<Bitmap>) -> Box<dyn Array> {
        Box::new(VecBackedArray {
            values: self.values.clone(),
            validity,
        })
    }

    fn to_boxed(&self) -> Box<dyn Array> {
        Box::new(VecBackedArray {
            values: self.values.clone(),
            validity: self.validity.clone(),
        })
    }
}
