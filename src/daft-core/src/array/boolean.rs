use arrow::buffer::BooleanBuffer;

use super::ops::as_arrow::AsArrow;
use crate::datatypes::BooleanArray;

impl BooleanArray {
    pub fn to_bitmap(&self) -> BooleanBuffer {
        self.to_arrow()
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .expect("BooleanArray data is always boolean")
            .values()
            .clone()
    }
    pub fn false_count(&self) -> usize {
        self.as_arrow().expect("BooleanArray").false_count()
    }
    pub fn true_count(&self) -> usize {
        self.as_arrow().expect("BooleanArray").true_count()
    }
}
