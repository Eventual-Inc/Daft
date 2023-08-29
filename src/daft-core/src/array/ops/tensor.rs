use crate::{array::ops::as_arrow::AsArrow, datatypes::logical::TensorArray};

impl TensorArray {
    pub fn data_array(&self) -> &arrow2::array::ListArray<i64> {
        const DATA_IDX: usize = 0;
        let array = self.physical.children.get(DATA_IDX).unwrap();
        array.list().unwrap().as_arrow()
    }

    pub fn shape_array(&self) -> &arrow2::array::ListArray<i64> {
        const SHAPE_IDX: usize = 1;
        let array = self.physical.children.get(SHAPE_IDX).unwrap();
        array.list().unwrap().as_arrow()
    }
}
