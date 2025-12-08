use std::sync::Arc;

use arrow_array::Array as ArrowArray;
use arrow_buffer::{BooleanBuffer, NullBuffer};
use crate::datatypes::DataType;

use crate::{impl_array_common, array::base::{Array, ArrayRef}};


#[derive(Debug)]
pub struct BooleanArray(pub(crate) arrow_array::BooleanArray);

impl BooleanArray {
    pub fn new(values: BooleanBuffer, nulls: Option<NullBuffer>) -> Self {
        Self(arrow_array::BooleanArray::new(values, nulls))
    }
    pub fn new_null(len: usize) -> Self {
        Self(arrow_array::BooleanArray::new_null(len))
    }
    pub fn value(&self, index: usize) -> bool {
        self.0.value(index)
    }
    pub unsafe fn value_unchecked(&self, index: usize) -> bool {
        unsafe { self.0.value_unchecked(index) }
    }
    pub fn values(&self) -> &BooleanBuffer {
        self.0.values()
    }
}

impl Array for BooleanArray {
    impl_array_common!();
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(Self(self.0.slice(offset, length)))
    }
    fn data_type(&self) -> &DataType {
        &DataType::Boolean
    }
}

impl From<Vec<Option<bool>>> for BooleanArray {
    fn from(data: Vec<Option<bool>>) -> Self {
        Self(arrow_array::BooleanArray::from(data))
    }
}
