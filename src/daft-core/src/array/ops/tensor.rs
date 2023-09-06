use crate::{array::ListArray, datatypes::logical::TensorArray};

impl TensorArray {
    pub fn data_array(&self) -> &ListArray {
        const DATA_IDX: usize = 0;
        let array = self.physical.children.get(DATA_IDX).unwrap();
        array.list().unwrap()
    }

    pub fn shape_array(&self) -> &ListArray {
        const SHAPE_IDX: usize = 1;
        let array = self.physical.children.get(SHAPE_IDX).unwrap();
        array.list().unwrap()
    }
}
