use std::sync::Arc;

use arrow_array::Array as ArrowArray;
use arrow2::datatypes::DataType;

use crate::{impl_array_common, array::base::{Array, ArrayRef}};

#[derive(Debug)]
pub struct NullArray(pub(crate) arrow_array::NullArray);

impl NullArray {
    pub fn new(length: usize) -> Self {
        Self(arrow_array::NullArray::new(length))
    }
}

impl Array for NullArray {
    impl_array_common!();
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(Self(self.0.slice(offset, length)))
    }
    fn data_type(&self) -> &DataType {
        &DataType::Null
    }
}
