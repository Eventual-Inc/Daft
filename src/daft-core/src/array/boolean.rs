use crate::datatypes::BooleanArray;

use super::ops::as_arrow::AsArrow;

impl BooleanArray {
    pub fn as_bitmap(&self) -> &arrow2::bitmap::Bitmap {
        self.as_arrow().values()
    }
}
