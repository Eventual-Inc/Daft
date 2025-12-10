use std::sync::Arc;

use arrow_array::{Array as ArrowArray, ArrowPrimitiveType};
use arrow_buffer::{NullBuffer, ScalarBuffer};
use arrow2::datatypes::DataType;

use crate::{impl_array_common, array::base::{Array, ArrayRef}};


pub struct PrimitiveArray<T: ArrowPrimitiveType>(
    pub(crate) arrow_array::PrimitiveArray<T>,
    pub(crate) DataType,
);

impl<T: ArrowPrimitiveType> std::fmt::Debug for PrimitiveArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl<T: ArrowPrimitiveType> PrimitiveArray<T> {
    pub fn new(values: ScalarBuffer<T::Native>, nulls: Option<NullBuffer>) -> Self {
        let data_type = T::DATA_TYPE.into();
        Self(
            arrow_array::PrimitiveArray::new(values, nulls),
            data_type,
        )
    }
    pub fn values(&self) -> &ScalarBuffer<T::Native> {
        self.0.values()
    }
    pub fn value(&self, i: usize) -> T::Native {
        self.0.value(i)
    }
    pub unsafe fn value_unchecked(&self, i: usize) -> T::Native {
        unsafe { self.0.value_unchecked(i) }
    }
}

impl<T: ArrowPrimitiveType> Array for PrimitiveArray<T> {
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

impl<'a, T: ArrowPrimitiveType> IntoIterator for &'a PrimitiveArray<T> {
    type Item = Option<<T as ArrowPrimitiveType>::Native>;
    type IntoIter = arrow_array::iterator::PrimitiveIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
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
pub type MonthsDaysNsArray = PrimitiveArray<arrow_array::types::IntervalMonthDayNanoType>;
