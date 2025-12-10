use std::sync::Arc;

use arrow_array::{Array as ArrowArray, OffsetSizeTrait};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};
use arrow2::datatypes::DataType;

use crate::{impl_array_common, array::base::{Array, ArrayRef}};


#[derive(Debug)]
pub struct GenericStringArray<O: OffsetSizeTrait>(pub(crate) arrow_array::GenericStringArray<O>);

impl<O: OffsetSizeTrait> GenericStringArray<O> {
    pub fn new(offsets: OffsetBuffer<O>, values: Buffer, nulls: Option<NullBuffer>) -> Self {
        Self(arrow_array::GenericStringArray::new(offsets, values, nulls))
    }
}

impl<O: OffsetSizeTrait> Array for GenericStringArray<O> {
    impl_array_common!();
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(Self(self.0.slice(offset, length)))
    }
    fn data_type(&self) -> &DataType {
        if O::IS_LARGE {
            &DataType::LargeUtf8
        } else {
            &DataType::Utf8
        }
    }
}

pub type StringArray = GenericStringArray<i32>;
pub type LargeStringArray = GenericStringArray<i64>;


#[derive(Debug)]
pub struct GenericStringBuilder<O: OffsetSizeTrait>(pub(crate) arrow_array::builder::GenericStringBuilder<O>);

impl<O: OffsetSizeTrait> GenericStringBuilder<O> {
    pub fn new() -> Self {
        Self(arrow_array::builder::GenericStringBuilder::<O>::new())
    }
    pub fn with_capacity(item_capacity: usize, offset_capacity: usize) -> Self {
        Self(arrow_array::builder::GenericStringBuilder::<O>::with_capacity(item_capacity, offset_capacity))
    }
}

pub type StringBuilder = GenericStringBuilder<i32>;
pub type LargeStringBuilder = GenericStringBuilder<i64>;
