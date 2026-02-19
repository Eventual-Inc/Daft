use arrow::buffer::BooleanBuffer;

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
}
