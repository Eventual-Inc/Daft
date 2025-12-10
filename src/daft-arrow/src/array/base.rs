use std::any::Any;
use std::sync::Arc;

use crate::datatypes::DataType;

pub type ArrayRef = Arc<dyn Array>;
pub type ArrayData = arrow_data::ArrayData;
type NullBuffer = arrow_buffer::NullBuffer;

pub trait Array: std::fmt::Debug + Send + Sync {
    // Same as arrow_array::Array
    fn as_any(&self) -> &dyn Any;
    fn to_data(&self) -> ArrayData;
    fn into_data(self) -> ArrayData;
    fn slice(&self, offset: usize, length: usize) -> ArrayRef;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn shrink_to_fit(&mut self) {}
    fn offset(&self) -> usize;
    fn nulls(&self) -> Option<&NullBuffer>;
    fn logical_nulls(&self) -> Option<NullBuffer> {
        self.nulls().cloned()
    }
    fn is_null(&self, index: usize) -> bool {
        self.nulls().map(|n| n.is_null(index)).unwrap_or_default()
    }
    fn is_valid(&self, index: usize) -> bool {
        !self.is_null(index)
    }
    fn null_count(&self) -> usize {
        self.nulls().map(|n| n.null_count()).unwrap_or_default()
    }
    fn logical_null_count(&self) -> usize {
        self.logical_nulls()
            .map(|n| n.null_count())
            .unwrap_or_default()
    }
    fn is_nullable(&self) -> bool {
        self.logical_null_count() != 0
    }
    fn get_buffer_memory_size(&self) -> usize;
    fn get_array_memory_size(&self) -> usize;

    // Modified
    // Returns a arrow2::datatypes::DataType
    fn data_type(&self) -> &DataType;
}

#[macro_export]
macro_rules! impl_array_common {
    () => {
        fn as_any(&self) -> &dyn std::any::Any {
            self.0.as_any()
        }
        fn to_data(&self) -> arrow_data::ArrayData {
            self.0.to_data()
        }
        fn into_data(self) -> arrow_data::ArrayData {
            self.0.to_data()
        }
        fn len(&self) -> usize {
            self.0.len()
        }
        fn is_empty(&self) -> bool {
            self.0.is_empty()
        }
        fn offset(&self) -> usize {
            self.0.offset()
        }
        fn nulls(&self) -> Option<&arrow_buffer::NullBuffer> {
            self.0.nulls()
        }
        fn get_buffer_memory_size(&self) -> usize {
            self.0.get_buffer_memory_size()
        }
        fn get_array_memory_size(&self) -> usize {
            self.0.get_array_memory_size()
        }
    }
}
