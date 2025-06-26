use super::ops::as_arrow::AsArrow;
use crate::datatypes::BooleanArray;

impl BooleanArray {
    pub fn as_bitmap(&self) -> &arrow2::bitmap::Bitmap {
        self.as_arrow().values()
    }
}
