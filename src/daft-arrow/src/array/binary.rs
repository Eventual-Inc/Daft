use std::sync::Arc;

use arrow_array::{Array as ArrowArray, OffsetSizeTrait};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};
use arrow2::datatypes::DataType;

use crate::{impl_array_common, array::base::{Array, ArrayRef}};


#[derive(Debug)]
pub struct GenericBinaryArray<O: OffsetSizeTrait>(pub(crate) arrow_array::GenericBinaryArray<O>);

impl<O: OffsetSizeTrait> GenericBinaryArray<O> {
    pub fn new(offsets: OffsetBuffer<O>, values: Buffer, nulls: Option<NullBuffer>) -> Self {
        Self(arrow_array::GenericBinaryArray::new(offsets, values, nulls))
    }
}

impl<O: OffsetSizeTrait> Array for GenericBinaryArray<O> {
    impl_array_common!();
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(Self(self.0.slice(offset, length)))
    }
    fn data_type(&self) -> &DataType {
        if O::IS_LARGE {
            &DataType::LargeBinary
        } else {
            &DataType::Binary
        }
    }
}

pub type LargeBinaryArray = GenericBinaryArray<i64>;

#[derive(Debug)]
pub struct FixedSizeBinaryArray(pub(crate) arrow_array::FixedSizeBinaryArray, pub(crate) DataType);

impl FixedSizeBinaryArray {
    pub fn new(size: i32, values: Buffer, nulls: Option<NullBuffer>) -> Self {
        Self(
            arrow_array::FixedSizeBinaryArray::new(size, values, nulls),
            DataType::FixedSizeBinary(size as usize),
        )
    }
}

impl Array for FixedSizeBinaryArray {
    impl_array_common!();
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(Self(self.0.slice(offset, length), self.1.clone()))
    }
    fn data_type(&self) -> &DataType {
        &self.1
    }
}

#[derive(Debug)]
pub struct GenericBinaryBuilder<O: OffsetSizeTrait>(pub(crate) arrow_array::builder::GenericBinaryBuilder<O>);

impl<O: OffsetSizeTrait> GenericBinaryBuilder<O> {
    pub fn new() -> Self {
        Self(arrow_array::builder::GenericBinaryBuilder::<O>::new())
    }
    pub fn with_capacity(item_capacity: usize, offset_capacity: usize) -> Self {
        Self(arrow_array::builder::GenericBinaryBuilder::<O>::with_capacity(item_capacity, offset_capacity))
    }
}

pub type BinaryBuilder = GenericBinaryBuilder<i32>;
pub type LargeBinaryBuilder = GenericBinaryBuilder<i64>;
