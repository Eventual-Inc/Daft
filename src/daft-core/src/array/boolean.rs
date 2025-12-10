use super::ops::as_arrow::AsArrow;
use crate::datatypes::BooleanArray;

impl BooleanArray {
    pub fn as_buffer(&self) -> &daft_arrow::buffer::BooleanBuffer {
        self.as_arrow().values()
    }
}
