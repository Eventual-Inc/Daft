use crate::{array::ops::as_arrow::AsArrow, datatypes::logical::TensorArray};

impl TensorArray {
    pub fn data_array(&self) -> &arrow2::array::ListArray<i64> {
        let p = self.physical.as_arrow();
        const DATA_IDX: usize = 0;
        let array = p.values().get(DATA_IDX).unwrap();
        array.as_ref().as_any().downcast_ref().unwrap()
    }

    pub fn shape_array(&self) -> &arrow2::array::ListArray<i64> {
        let p = self.physical.as_arrow();
        const SHAPE_IDX: usize = 1;
        let array = p.values().get(SHAPE_IDX).unwrap();
        array.as_ref().as_any().downcast_ref().unwrap()
    }
}
