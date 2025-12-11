use super::ops::as_arrow::AsArrow;
use crate::datatypes::BooleanArray;

impl BooleanArray {
    pub fn as_bitmap(&self) -> &daft_arrow::bitmap::Bitmap {
        self.as_arrow2().values()
    }
}
