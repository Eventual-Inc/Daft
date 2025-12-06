use std::sync::Arc;

use arrow_array::{Array as ArrowArray, ArrowPrimitiveType};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow2::datatypes::DataType;

use crate::{impl_array_common, array::base::{Array, ArrayRef}};


#[derive(Debug)]
pub struct PrimitiveArray<T: ArrowPrimitiveType>(
    pub(crate) arrow_array::PrimitiveArray<T>,
    pub(crate) DataType,
);

impl<T: ArrowPrimitiveType> PrimitiveArray<T> {
    pub fn new(values: ScalarBuffer<T::Native>, nulls: Option<NullBuffer>) -> Self {
        let data_type = T::DATA_TYPE.into();
        Self(
            arrow_array::PrimitiveArray::new(values, nulls),
            data_type,
        )
    }
}

impl<T: ArrowPrimitiveType + std::fmt::Debug> Array for PrimitiveArray<T> {
    impl_array_common!();
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(Self(self.0.slice(offset, length), self.1.clone()))
    }
    fn data_type(&self) -> &DataType {
        &self.1
    }
}

impl<'a, T: ArrowPrimitiveType> PrimitiveArray<T> {
    /// constructs a new iterator
    pub fn iter(&'a self) -> arrow_array::iterator::PrimitiveIter<'a, T> {
        self.0.iter()
    }
}

pub type Int8Array = PrimitiveArray<arrow_array::types::Int8Type>;
pub type Int16Array = PrimitiveArray<arrow_array::types::Int16Type>;
pub type Int32Array = PrimitiveArray<arrow_array::types::Int32Type>;
pub type Int64Array = PrimitiveArray<arrow_array::types::Int64Type>;

pub type UInt8Array = PrimitiveArray<arrow_array::types::UInt8Type>;
pub type UInt16Array = PrimitiveArray<arrow_array::types::UInt16Type>;
pub type UInt32Array = PrimitiveArray<arrow_array::types::UInt32Type>;
pub type UInt64Array = PrimitiveArray<arrow_array::types::UInt64Type>;

pub type Float16Array = PrimitiveArray<arrow_array::types::Float16Type>;
pub type Float32Array = PrimitiveArray<arrow_array::types::Float32Type>;
pub type Float64Array = PrimitiveArray<arrow_array::types::Float64Type>;

pub type Decimal128Array = PrimitiveArray<arrow_array::types::Decimal128Type>;

#[derive(Debug)]
pub struct PrimitiveBuilder<T: ArrowPrimitiveType>(pub(crate) arrow_array::builder::PrimitiveBuilder<T>);

impl<T: ArrowPrimitiveType> PrimitiveBuilder<T> {
    pub fn new() -> Self {
        Self(arrow_array::builder::PrimitiveBuilder::<T>::new())
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self(arrow_array::builder::PrimitiveBuilder::<T>::with_capacity(capacity))
    }
}

pub type Int8Builder = PrimitiveBuilder<arrow_array::types::Int8Type>;
pub type Int16Builder = PrimitiveBuilder<arrow_array::types::Int16Type>;
pub type Int32Builder = PrimitiveBuilder<arrow_array::types::Int32Type>;
pub type Int64Builder = PrimitiveBuilder<arrow_array::types::Int64Type>;

pub type UInt8Builder = PrimitiveBuilder<arrow_array::types::UInt8Type>;
pub type UInt16Builder = PrimitiveBuilder<arrow_array::types::UInt16Type>;
pub type UInt32Builder = PrimitiveBuilder<arrow_array::types::UInt32Type>;
pub type UInt64Builder = PrimitiveBuilder<arrow_array::types::UInt64Type>;

pub type Float16Builder = PrimitiveBuilder<arrow_array::types::Float16Type>;
pub type Float32Builder = PrimitiveBuilder<arrow_array::types::Float32Type>;
pub type Float64Builder = PrimitiveBuilder<arrow_array::types::Float64Type>;

pub type Decimal128Builder = PrimitiveBuilder<arrow_array::types::Decimal128Type>;
