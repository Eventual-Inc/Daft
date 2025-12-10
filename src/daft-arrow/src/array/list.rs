use std::sync::Arc;

use arrow_array::{Array as ArrowArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow2::datatypes::{DataType, Field};

use crate::{impl_array_common, array::base::{Array, ArrayRef}};


#[derive(Debug)]
pub struct GenericListArray<O: arrow_array::OffsetSizeTrait>(
    pub(crate) arrow_array::GenericListArray<O>,
    pub(crate) DataType,
);

impl<O: arrow_array::OffsetSizeTrait> GenericListArray<O> {
    pub fn new(field: Box<Field>, offsets: OffsetBuffer<O>, values: Arc<dyn Array>, nulls: Option<NullBuffer>) -> Self {
        Self(
            arrow_array::GenericListArray::new(todo!(), offsets, todo!(), nulls),
            if O::IS_LARGE { DataType::LargeList(field) } else { DataType::List(field) },
        )
    }
}

impl<O: arrow_array::OffsetSizeTrait> Array for GenericListArray<O> {
    impl_array_common!();
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(Self(self.0.slice(offset, length), self.1.clone()))
    }
    fn data_type(&self) -> &DataType {
        &self.1
    }
}

pub type LargeListArray = GenericListArray<i64>;


#[derive(Debug)]
pub struct FixedSizeListArray(
    pub(crate) arrow_array::FixedSizeListArray,
    pub(crate) DataType,
);

impl FixedSizeListArray {
    pub fn new(field: Box<Field>, size: i32, values: Arc<dyn Array>, nulls: Option<NullBuffer>) -> Self {
        Self(
            arrow_array::FixedSizeListArray::new(todo!(), size, todo!(), nulls),
            DataType::FixedSizeList(field, size as usize),
        )
    }
    pub fn values(&self) -> &ArrayRef {
        let arrow_arr = self.0.values().clone();
        todo!()
    }
}

impl Array for FixedSizeListArray {
    impl_array_common!();
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(Self(self.0.slice(offset, length), self.1.clone()))
    }
    fn data_type(&self) -> &DataType {
        &self.1
    }
}
