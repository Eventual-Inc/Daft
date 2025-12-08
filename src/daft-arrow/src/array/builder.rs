use crate::array::NullArray;
use crate::array::BooleanArray;

#[derive(Debug)]
pub struct NullBuilder(pub(crate) arrow_array::builder::NullBuilder);

impl NullBuilder {
    pub fn new() -> Self {
        Self(arrow_array::builder::NullBuilder::new())
    }

    pub fn finish(&mut self) -> NullArray {
        NullArray(self.0.finish())
    }
}


#[derive(Debug)]
pub struct BooleanBuilder(pub(crate) arrow_array::builder::BooleanBuilder);

impl BooleanBuilder {
    pub fn new() -> Self {
        Self(arrow_array::builder::BooleanBuilder::new())
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self(arrow_array::builder::BooleanBuilder::with_capacity(capacity))
    }
    pub fn finish(&mut self) -> BooleanArray {
        BooleanArray(self.0.finish())
    }
}

use arrow_array::ArrowPrimitiveType;
pub use arrow_array::builder::ArrayBuilder;


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
