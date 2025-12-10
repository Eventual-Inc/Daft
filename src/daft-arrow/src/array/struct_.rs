use std::sync::Arc;

use arrow_array::{Array as ArrowArray};
use arrow_buffer::{NullBuffer};
use arrow2::datatypes::DataType;

use crate::{impl_array_common, array::base::{Array, ArrayRef}};


#[derive(Debug)]
pub struct StructArray(pub(crate) arrow_array::StructArray);

impl StructArray {
    pub fn new(fields: Vec<Arc<arrow2::datatypes::Field>>, arrays: Vec<ArrayRef>, nulls: Option<NullBuffer>) -> Self {
        Self(arrow_array::StructArray::new(todo!(), todo!(), nulls))
    }
}

impl Array for StructArray {
    impl_array_common!();
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(Self(self.0.slice(offset, length)))
    }
    fn data_type(&self) -> &DataType {
        &DataType::Utf8
    }
}
