use arrow::buffer::BooleanBuffer;

use super::ops::as_arrow::AsArrow;
use crate::datatypes::BooleanArray;

impl BooleanArray {
    pub fn values(&self) -> BooleanBuffer {
        self.as_arrow().unwrap().values().clone()
    }
}
