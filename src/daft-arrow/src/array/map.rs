use std::sync::Arc;

use arrow_array::{Array as ArrowArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow2::datatypes::{DataType, Field};

use crate::{array::{StructArray, base::{Array, ArrayRef}}, impl_array_common};


#[derive(Debug)]
pub struct MapArray(pub(crate) arrow_array::MapArray);

impl MapArray {
    pub fn new(field: Box<Field>, offsets: OffsetBuffer<i32>, entries: StructArray, nulls: Option<NullBuffer>, ordered: bool) -> Self {
        Self(arrow_array::MapArray::new(todo!(), offsets, todo!(), nulls, ordered))
    }
}

impl Array for MapArray {
    impl_array_common!();
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(Self(self.0.slice(offset, length)))
    }
    fn data_type(&self) -> &DataType {
        &DataType::Utf8
    }
}
